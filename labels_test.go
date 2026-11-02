package main

import (
	"reflect"
	"testing"
)

func TestParseLabels(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want map[string]string
	}{
		{name: "empty", in: "", want: map[string]string{}},
		{name: "single", in: "cluster=prod", want: map[string]string{"cluster": "prod"}},
		{name: "multiple", in: "cluster=prod,env=eu", want: map[string]string{"cluster": "prod", "env": "eu"}},
		{name: "value with equals sign", in: "dsn=a=b,env=eu", want: map[string]string{"dsn": "a=b", "env": "eu"}},
		{name: "entry without equals sign is ignored", in: "cluster,env=eu", want: map[string]string{"env": "eu"}},
		{name: "empty value", in: "cluster=", want: map[string]string{"cluster": ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseLabels(tt.in); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseLabels(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
