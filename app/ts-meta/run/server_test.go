// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package run

import (
	"net"
	"path"
	"strings"
	"testing"
	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/openGemini/openGemini/app"
	"github.com/openGemini/openGemini/app/ts-meta/meta"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	"github.com/stretchr/testify/require"
)

func Test_NewServer(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	// case 1: nil config
	server, err := NewServer(nil, app.ServerInfo{}, log)
	require.EqualError(t, err, "invalid meta config")

	// case 2: normal config
	conf := config.NewTSMeta(true)
	conf.Data.DataDir = path.Join(tmpDir, "data")
	conf.Data.MetaDir = path.Join(tmpDir, "meta")
	conf.Data.WALDir = path.Join(tmpDir, "wal")
	conf.Meta.Dir = path.Join(tmpDir, "meta")
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err = NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaService)
	require.NotNil(t, server.(*Server).sherlockService)
}

func Test_NewServer_Open_Close(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	// case 1: nil config
	server, err := NewServer(nil, app.ServerInfo{}, log)
	require.EqualError(t, err, "invalid meta config")

	// case 2: normal config
	conf := config.NewTSMeta(true)
	if len(conf.Common.MetaJoin) == 0 {
		conf.Common.MetaJoin = append(conf.Common.MetaJoin, []string{"127.0.0.1:9192"}...)
	}
	conf.Common.ReportEnable = false
	conf.Common.PprofEnabled = false
	conf.Gossip.Enabled = false

	// RPCBindAddress must use a specific port (not :0) because the raft
	// configuration derives LocalID and peer addresses from it. Using :0
	// causes the AddrRewriter to replace it with the mux listener address,
	// which breaks raft peer/LocalID matching.
	conf.Meta.BindAddress = "127.0.0.1:0"
	conf.Meta.HTTPBindAddress = "127.0.0.1:0"
	conf.Meta.RPCBindAddress = "127.0.0.1:8092"
	conf.Meta.Dir = path.Join(tmpDir, "meta")

	conf.Data.DataDir = path.Join(tmpDir, "data")
	conf.Data.MetaDir = path.Join(tmpDir, "meta")
	conf.Data.WALDir = path.Join(tmpDir, "wal")
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err = NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	s := server.(*Server)
	require.NotNil(t, s.MetaService)
	require.NotNil(t, s.sherlockService)

	// Pre-set a mock listener to avoid binding BindAddress to a hardcoded port.
	// The mux and RaftListener are still created normally from this listener.
	s.Listener = newMockListener(t)

	err = server.Open()
	require.NoError(t, err)

	err = server.Close()
	require.NoError(t, err)
}

func Test_NewServer_Open_Close_IncSyncMetaData(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	// case 1: nil config
	server, err := NewServer(nil, app.ServerInfo{}, log)
	require.EqualError(t, err, "invalid meta config")

	// case 2: normal config
	conf := config.NewTSMeta(true)
	if len(conf.Common.MetaJoin) == 0 {
		conf.Common.MetaJoin = append(conf.Common.MetaJoin, []string{"127.0.0.1:9192"}...)
	}
	conf.Common.ReportEnable = false
	conf.Common.PprofEnabled = false
	conf.Gossip.Enabled = false

	// RPCBindAddress must use a specific port (not :0) because the raft
	// configuration derives LocalID and peer addresses from it. Using :0
	// causes the AddrRewriter to replace it with the mux listener address,
	// which breaks raft peer/LocalID matching.
	conf.Meta.BindAddress = "127.0.0.1:0"
	conf.Meta.HTTPBindAddress = "127.0.0.1:0"
	conf.Meta.RPCBindAddress = "127.0.0.1:8092"
	conf.Meta.Dir = path.Join(tmpDir, "meta")
	conf.Meta.UseIncSyncData = true

	conf.Data.DataDir = path.Join(tmpDir, "data")
	conf.Data.MetaDir = path.Join(tmpDir, "meta")
	conf.Data.WALDir = path.Join(tmpDir, "wal")
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")

	server, err = NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	s := server.(*Server)
	require.NotNil(t, s.MetaService)
	require.NotNil(t, s.sherlockService)

	// Pre-set a mock listener to avoid binding BindAddress to a hardcoded port.
	// The mux and RaftListener are still created normally from this listener.
	s.Listener = newMockListener(t)

	err = server.Open()
	require.NoError(t, err)

	err = server.Close()
	require.NoError(t, err)
}

func Test_NewServer_Statistics(t *testing.T) {
	tsMetaconfig := config.NewTSMeta(true)
	tsMetaconfig.Monitor.StoreEnabled = true
	tsMetaconfig.Monitor.Pushers = "http"

	server := &Server{}
	conf := config.NewMeta()
	server.MetaService = meta.NewService(conf, nil, nil)
	server.config = tsMetaconfig
	server.initStatisticsPusher()
	puser := &statisticsPusher.StatisticsPusher{}
	server.MetaService.SetStatisticsPusher(puser)

	config.SetProductType("logkeeper")
	server.initStatisticsPusher()
}

func Test_NewServer_Statistics_Single(t *testing.T) {
	tsMetaconfig := config.NewTSMeta(false)
	tsMetaconfig.Monitor.StoreEnabled = true
	tsMetaconfig.Monitor.Pushers = "http"

	server := &Server{}
	conf := config.NewMeta()
	server.info.App = config.AppSingle
	server.MetaService = meta.NewService(conf, nil, nil)
	server.config = tsMetaconfig
	server.initStatisticsPusher()
	pusher := &statisticsPusher.StatisticsPusher{}
	server.MetaService.SetStatisticsPusher(pusher)
}

func Test_NewServer_Statistics_Data(t *testing.T) {
	tsMetaconfig := config.NewTSMeta(true)
	tsMetaconfig.Monitor.StoreEnabled = true
	tsMetaconfig.Monitor.Pushers = "http"

	server := &Server{}
	conf := config.NewMeta()
	server.info.App = config.AppData
	server.MetaService = meta.NewService(conf, nil, nil)
	server.config = tsMetaconfig
	server.initStatisticsPusher()
	pusher := &statisticsPusher.StatisticsPusher{}
	server.MetaService.SetStatisticsPusher(pusher)
}

func TestNewServer1(t *testing.T) {
	tsMetaconfig := config.NewTSMeta(true)
	tsMetaconfig.Meta.Dir = t.TempDir()
	tsMetaconfig.Monitor.StoreEnabled = false
	tsMetaconfig.Monitor.Pushers = "http"

	_, err := NewServer(tsMetaconfig, app.ServerInfo{}, nil)
	if err != nil {
		t.Fatal("NewServer fail")
	}
}

func TestNewCommand(t *testing.T) {
	cmd := NewCommand(app.ServerInfo{App: config.AppMeta}, false)
	require.Equal(t, app.METALOGO, cmd.Logo)
	require.Equal(t, config.AppMeta, cmd.Info.App)
}

func TestNewServerWithTlsUseCase(t *testing.T) {
	tmpDir := t.TempDir()

	log := logger.NewLogger(errno.ModuleUnknown)

	// case 2: normal config
	conf := config.NewTSMeta(true)
	conf.Data.DataDir = path.Join(tmpDir, "data")
	conf.Data.MetaDir = path.Join(tmpDir, "meta")
	conf.Data.WALDir = path.Join(tmpDir, "wal")
	conf.Meta.Dir = path.Join(tmpDir, "meta")
	conf.Sherlock.DumpPath = path.Join(tmpDir, "sherlock")
	// normal config
	conf.TLS = tlsconfig.Config{
		Ciphers:    nil,
		MinVersion: "",
		MaxVersion: "",
	}

	server, err := NewServer(conf, app.ServerInfo{}, log)
	require.NoError(t, err)
	require.NotNil(t, server.(*Server).MetaService)
	require.NotNil(t, server.(*Server).sherlockService)

	// abnormal config
	conf.TLS = tlsconfig.Config{
		Ciphers: []string{
			"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384_not_exist",
		},
		MinVersion: "",
		MaxVersion: "",
	}
	server, err = NewServer(conf, app.ServerInfo{}, log)
	require.Nil(t, server)
	require.Error(t, err)
	require.EqualValues(t, true, strings.HasPrefix(err.Error(),
		"parse tls config failed"))
}

// newMockListener creates a TCP listener on a random port (127.0.0.1:0).
// Tests use this to inject pre-set listeners before Open(), so the server
// never binds to hardcoded ports.
func newMockListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return ln
}
