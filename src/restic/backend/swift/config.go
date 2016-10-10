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
	ApiKey       string
	AuthUrl      string
	Region       string
	Tenant       string
	TenantId     string
	TenantDomain string
	TrustId      string

	StorageUrl string
	AuthToken  string

	Container              string
	Prefix                 string
	DefaultContainerPolicy string
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
		{&cfg.ApiKey, "OS_PASSWORD"},
		{&cfg.Region, "OS_REGION_NAME"},
		{&cfg.AuthUrl, "OS_AUTH_URL"},

		// v3 specific
		{&cfg.Domain, "OS_USER_DOMAIN_NAME"},
		{&cfg.Tenant, "OS_PROJECT_NAME"},
		{&cfg.TenantDomain, "OS_PROJECT_DOMAIN_NAME"},

		// v2 specific
		{&cfg.TenantId, "OS_TENANT_ID"},
		{&cfg.Tenant, "OS_TENANT_NAME"},

		// v1 specific
		{&cfg.AuthUrl, "ST_AUTH"},
		{&cfg.UserName, "ST_USER"},
		{&cfg.ApiKey, "ST_KEY"},

		// Manual authentication
		{&cfg.StorageUrl, "OS_STORAGE_URL"},
		{&cfg.AuthToken, "OS_AUTH_TOKEN"},

		{&cfg.DefaultContainerPolicy, "SWIFT_DEFAULT_CONTAINER_POLICY"},
	} {
		if *val.s == "" {
			*val.s = os.Getenv(val.env)
		}
	}

	return cfg, nil
}
