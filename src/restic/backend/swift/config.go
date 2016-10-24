package swift

import (
	"net/url"
	"os"
	"restic/errors"
	"strings"
)

// Config contains basic configuration needed to specify swift location for an
// swift server
type Config struct {
	UserName     string
	Domain       string
	APIKey       string
	AuthURL      string
	Region       string
	Tenant       string
	TenantID     string
	TenantDomain string
	TrustID      string

	StorageURL string
	AuthToken  string

	Container              string
	Prefix                 string
	DefaultContainerPolicy string
}

// ParseConfig parses the string s and extract swift's container name and
// prefix. The string must be of form: swift:///container/prefix.
// In addition to parsing configuration this function extracts swift
// endpoint configuration from environment variables.
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

	cfg := Config{
		Container: parts[1],
	}

	if len(parts) > 2 {
		cfg.Prefix = parts[2]
	}

	for _, val := range []struct {
		s   *string
		env string
	}{
		// v2/v3 specific
		{&cfg.UserName, "OS_USERNAME"},
		{&cfg.APIKey, "OS_PASSWORD"},
		{&cfg.Region, "OS_REGION_NAME"},
		{&cfg.AuthURL, "OS_AUTH_URL"},

		// v3 specific
		{&cfg.Domain, "OS_USER_DOMAIN_NAME"},
		{&cfg.Tenant, "OS_PROJECT_NAME"},
		{&cfg.TenantDomain, "OS_PROJECT_DOMAIN_NAME"},

		// v2 specific
		{&cfg.TenantID, "OS_TENANT_ID"},
		{&cfg.Tenant, "OS_TENANT_NAME"},

		// v1 specific
		{&cfg.AuthURL, "ST_AUTH"},
		{&cfg.UserName, "ST_USER"},
		{&cfg.APIKey, "ST_KEY"},

		// Manual authentication
		{&cfg.StorageURL, "OS_STORAGE_URL"},
		{&cfg.AuthToken, "OS_AUTH_TOKEN"},

		{&cfg.DefaultContainerPolicy, "SWIFT_DEFAULT_CONTAINER_POLICY"},
	} {
		if *val.s == "" {
			*val.s = os.Getenv(val.env)
		}
	}

	return cfg, nil
}
