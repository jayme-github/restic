package swift

import (
	"net/url"
	"restic/errors"
	"strings"
)

// Config contains basic configuration needed to specify swift location for an
// swift server
type Config struct {
	Container string
	Prefix    string
}

func ParseConfig(s string) (interface{}, error) {

	url, err := url.Parse(s)
	if err != nil {
		return nil, errors.Wrap(err, "url.Parse")
	}

	if url.Host != "" {
		return nil, errors.New("swift: Hostname in swift url is not supported")
	}

	parts := strings.SplitN(url.Path, "/", 3)
	if len(parts) < 2 || len(parts[1]) == 0 {
		return nil, errors.New("swift: Missing container name")
	}

	cfg := Config{Container: parts[1]}
	if len(parts) > 2 {
		cfg.Prefix = parts[2]
	}

	return cfg, nil
}
