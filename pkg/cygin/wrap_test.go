package cygin

import "testing"

type getConfigParam struct {
	EnvId string `form:"env_id"  binding:"required"`
}

func TestServer(t *testing.T) {
	bindType, params := getBindFlagsFromValue(&getConfigParam{})
	t.Log(bindType)
	t.Log(params)
}
