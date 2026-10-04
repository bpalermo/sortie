package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"strings"
	"testing"
)

// packaged builds a gzipped tar the way `helm package` lays a chart out.
func packaged(t *testing.T, entries map[string]string) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, body := range entries {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func TestReadFindsTheTopLevelChartYamlOnly(t *testing.T) {
	pkg := packaged(t, map[string]string{
		"sortie/Chart.yaml":            "apiVersion: v2\nname: sortie\nversion: 0.1.0-abc\nappVersion: v0.1.0\ndescription: |\n  Runs a plan.\n",
		"sortie/values.yaml":           "image: {}\n",
		"sortie/charts/dep/Chart.yaml": "apiVersion: v2\nname: dep\nversion: 9.9.9\n",
	})
	c, err := Read(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "sortie" || c.Version != "0.1.0-abc" {
		t.Errorf("name/version = %q/%q", c.Name, c.Version)
	}
	var cfg map[string]any
	if err := json.Unmarshal(c.Config, &cfg); err != nil {
		t.Fatalf("config is not JSON: %v", err)
	}
	if cfg["appVersion"] != "v0.1.0" || !strings.HasPrefix(cfg["description"].(string), "Runs a plan.") {
		t.Errorf("config = %s", c.Config)
	}
}

func TestReadRejectsWhatIsNotOneChart(t *testing.T) {
	for name, entries := range map[string]map[string]string{
		"no Chart.yaml":  {"sortie/values.yaml": "x: 1\n"},
		"two charts":     {"a/Chart.yaml": "name: a\nversion: 1\n", "b/Chart.yaml": "name: b\nversion: 1\n"},
		"no name":        {"sortie/Chart.yaml": "version: 1.0.0\n"},
		"no version":     {"sortie/Chart.yaml": "name: sortie\n"},
		"not a mapping":  {"sortie/Chart.yaml": "- a\n- b\n"},
	} {
		if _, err := Read(packaged(t, entries)); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if _, err := Read(strings.NewReader("not gzip")); err == nil {
		t.Error("plain bytes: want an error")
	}
}
