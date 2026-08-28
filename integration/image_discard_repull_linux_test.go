/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	criruntime "k8s.io/cri-api/pkg/apis/runtime/v1"

	containerd "github.com/containerd/containerd/v2/client"
	containerdimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/integration/images"
	"github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/platforms"
)

// TestImageStatusDiscardedLayersSnapshotterSwitch verifies that an image pulled
// with discard_unpacked_layers=true is reported absent for a runtime handler
// whose snapshotter it was never unpacked into (blobs gone, no snapshot),
// while remaining present for the default snapshotter (snapshot intact), and
// that a re-pull restores the discarded blobs.
func TestImageStatusDiscardedLayersSnapshotterSwitch(t *testing.T) {
	workDir := t.TempDir()
	cfgPath := filepath.Join(workDir, "config.toml")
	cfg := `
version = 3

[plugins.'io.containerd.cri.v1.images']
  snapshotter = "overlayfs"
  discard_unpacked_layers = true

[plugins.'io.containerd.cri.v1.runtime'.containerd]
  default_runtime_name = "runc"

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"
  snapshotter = "overlayfs"

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.native]
  runtime_type = "io.containerd.runc.v2"
  snapshotter = "native"
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(cfg), 0o600))

	ctrd := newCtrdProc(t, *containerdBin, workDir, nil)
	require.NoError(t, ctrd.isReady())

	iSvc := ctrd.criImageService(t)
	rSvc := ctrd.criRuntimeService(t)

	ctrdClient, err := containerd.New(ctrd.grpcAddress(), containerd.WithDefaultNamespace(k8sNamespace))
	require.NoError(t, err)

	t.Cleanup(func() {
		if t.Failed() {
			t.Log("Dumping containerd config and logs due to test failure")
			dumpFileContent(t, ctrd.configPath())
			dumpFileContent(t, ctrd.logPath())
		}
		assert.NoError(t, ctrdClient.Close())
		cleanupPods(t, rSvc)
		assert.NoError(t, ctrd.kill(syscall.SIGTERM))
		assert.NoError(t, ctrd.wait(5*time.Minute))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The native snapshotter must be registered and healthy, otherwise the
	// snapshotter-switch scenario cannot be exercised.
	resp, err := ctrdClient.IntrospectionService().Plugins(ctx, fmt.Sprintf("type==%s,id==%s", plugins.SnapshotPlugin, "native"))
	require.NoError(t, err)
	if len(resp.Plugins) == 0 {
		t.Skip("native snapshotter plugin is not registered")
	}
	if initErr := resp.Plugins[0].InitErr; initErr != nil {
		t.Skipf("native snapshotter plugin is not ready: %s", initErr.Message)
	}

	testImage := images.Get(images.BusyBox)

	// 1. Pull into overlayfs; with discard_unpacked_layers=true the layer
	//    blobs become unreferenced in the content store.
	pullImagesByCRI(t, iSvc, testImage)

	img, err := ctrdClient.GetImage(context.Background(), testImage)
	require.NoError(t, err)

	diffIDs, err := img.RootFS(context.Background())
	require.NoError(t, err)
	chainIDs := identity.ChainIDs(diffIDs)

	manifest, err := containerdimages.Manifest(context.Background(), ctrdClient.ContentStore(), img.Target(), platforms.Default())
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Layers)

	// 2. Drop the now-unreferenced layer blobs, emulating GC.
	cs := ctrdClient.ContentStore()
	layerDigests := make([]digest.Digest, 0, len(manifest.Layers))
	for _, layer := range manifest.Layers {
		layerDigests = append(layerDigests, layer.Digest)
		if err := cs.Delete(context.Background(), layer.Digest); err != nil && !errdefs.IsNotFound(err) {
			require.NoErrorf(t, err, "failed to delete layer blob %s", layer.Digest)
		}
	}
	for _, dgst := range layerDigests {
		_, err := cs.Info(context.Background(), dgst)
		require.Truef(t, errdefs.IsNotFound(err), "layer blob %s should be gone, got err=%v", dgst, err)
	}

	// Sanity: the image was never unpacked into the native snapshotter.
	nativeSn := ctrdClient.SnapshotService("native")
	for _, chainID := range chainIDs {
		_, err := nativeSn.Stat(context.Background(), chainID.String())
		require.Truef(t, errdefs.IsNotFound(err), "unexpected native snapshot for chainID %s", chainID)
	}

	// 3. ImageStatus for the native handler must report absent (no native
	//    snapshot, blobs gone); the default overlayfs handler must report
	//    present (snapshot intact).
	nativeStatus, err := iSvc.ImageStatus(&criruntime.ImageSpec{Image: testImage, RuntimeHandler: "native"})
	require.NoError(t, err)
	assert.Nil(t, nativeStatus, "image unusable for the native snapshotter should be reported absent")

	overlayStatus, err := iSvc.ImageStatus(&criruntime.ImageSpec{Image: testImage, RuntimeHandler: ""})
	require.NoError(t, err)
	require.NotNil(t, overlayStatus, "image is still usable for the default overlayfs handler")

	// 4. Re-pull restores the discarded layer blobs to the content store.
	_, err = iSvc.PullImage(&criruntime.ImageSpec{Image: testImage}, nil, nil, "")
	require.NoError(t, err)

	for _, dgst := range layerDigests {
		_, err := cs.Info(context.Background(), dgst)
		require.NoErrorf(t, err, "layer blob %s should be restored to the content store after re-pull", dgst)
	}
}

// TestImageStatusDiscardedLayersNoRuntimeHandler covers the no-runtime-handler
// path (RuntimeClassInImageCriAPI disabled): the default snapshot exists so the
// snapshot check passes, leaving the blob check as the only signal that the
// image is unusable. With discard_unpacked_layers=false, missing blobs must
// report the image absent so kubelet re-pulls and restores them.
func TestImageStatusDiscardedLayersNoRuntimeHandler(t *testing.T) {
	workDir := t.TempDir()
	cfgPath := filepath.Join(workDir, "config.toml")
	cfg := `
version = 3

[plugins.'io.containerd.cri.v1.images']
  snapshotter = "overlayfs"
  discard_unpacked_layers = false

[plugins.'io.containerd.cri.v1.runtime'.containerd]
  default_runtime_name = "runc"

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"
  snapshotter = "overlayfs"
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(cfg), 0o600))

	ctrd := newCtrdProc(t, *containerdBin, workDir, nil)
	require.NoError(t, ctrd.isReady())

	iSvc := ctrd.criImageService(t)
	rSvc := ctrd.criRuntimeService(t)

	ctrdClient, err := containerd.New(ctrd.grpcAddress(), containerd.WithDefaultNamespace(k8sNamespace))
	require.NoError(t, err)

	t.Cleanup(func() {
		if t.Failed() {
			t.Log("Dumping containerd config and logs due to test failure")
			dumpFileContent(t, ctrd.configPath())
			dumpFileContent(t, ctrd.logPath())
		}
		assert.NoError(t, ctrdClient.Close())
		cleanupPods(t, rSvc)
		assert.NoError(t, ctrd.kill(syscall.SIGTERM))
		assert.NoError(t, ctrd.wait(5*time.Minute))
	})

	testImage := images.Get(images.BusyBox)

	// 1. Pull into overlayfs; blobs are retained (discard=false).
	pullImagesByCRI(t, iSvc, testImage)

	img, err := ctrdClient.GetImage(context.Background(), testImage)
	require.NoError(t, err)
	manifest, err := containerdimages.Manifest(context.Background(), ctrdClient.ContentStore(), img.Target(), platforms.Default())
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Layers)

	// 2. Emulate the corrupt state: snapshot intact, blobs gone.
	cs := ctrdClient.ContentStore()
	layerDigests := make([]digest.Digest, 0, len(manifest.Layers))
	for _, layer := range manifest.Layers {
		layerDigests = append(layerDigests, layer.Digest)
		if err := cs.Delete(context.Background(), layer.Digest); err != nil && !errdefs.IsNotFound(err) {
			require.NoErrorf(t, err, "failed to delete layer blob %s", layer.Digest)
		}
	}
	for _, dgst := range layerDigests {
		_, err := cs.Info(context.Background(), dgst)
		require.Truef(t, errdefs.IsNotFound(err), "layer blob %s should be gone, got err=%v", dgst, err)
	}

	// 3. ImageStatus with no handler must report absent: blobs gone,
	//    discard=false means they're expected present.
	status, err := iSvc.ImageStatus(&criruntime.ImageSpec{Image: testImage})
	require.NoError(t, err)
	assert.Nil(t, status, "image with discarded blobs (discard=false, no handler) should be reported absent")

	// 4. Re-pull heals it: the blobs are restored to the content store.
	_, err = iSvc.PullImage(&criruntime.ImageSpec{Image: testImage}, nil, nil, "")
	require.NoError(t, err)

	for _, dgst := range layerDigests {
		_, err := cs.Info(context.Background(), dgst)
		require.NoErrorf(t, err, "layer blob %s should be restored to the content store after re-pull", dgst)
	}

	status, err = iSvc.ImageStatus(&criruntime.ImageSpec{Image: testImage})
	require.NoError(t, err)
	require.NotNil(t, status, "image should be reported present again after re-pull")
}
