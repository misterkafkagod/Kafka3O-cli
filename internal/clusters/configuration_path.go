package clusters

import (
	"net/url"
	"strconv"
)

// ConfigurationPath is the Gateway path of the resource's configuration.
func ConfigurationPath(resource ConfigurationResource) string {
	if resource.Kind == "broker" {
		id, _ := strconv.Atoi(resource.Name)
		return "/api/v1/brokers/" + strconv.Itoa(id) + "/config"
	}
	return "/api/v1/topics/" + url.PathEscape(resource.Name) + "/config"
}
