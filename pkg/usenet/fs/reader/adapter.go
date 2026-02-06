package reader

import (
	"sync"

	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

var segmentMetaCache sync.Map // map[*types.Volume][]SegmentMeta

// VolumeToSegmentMeta converts a types.Volume to []SegmentMeta for the new reader.
func VolumeToSegmentMeta(vol *types.Volume) []SegmentMeta {
	if vol == nil || len(vol.Segments) == 0 {
		return nil
	}
	if cached, ok := segmentMetaCache.Load(vol); ok {
		return cached.([]SegmentMeta)
	}
	meta := NewSegmentMetaSlice(vol.Segments)
	segmentMetaCache.Store(vol, meta)
	return meta
}

// EncryptionFromVolume creates EncryptionConfig from a Volume's encryption settings.
func EncryptionFromVolume(vol *types.Volume) EncryptionConfig {
	if vol == nil || !vol.IsEncrypted {
		return EncryptionConfig{Enabled: false}
	}
	return EncryptionConfig{
		Enabled: true,
		Key:     vol.EncryptionKey,
		IV:      vol.EncryptionIV,
	}
}
