package tokens

import "github.com/gophercloud/gophercloud"

func CreateURL(client *gophercloud.ServiceClient) string {
	return client.ServiceURL("tokens")
}

func GetURL(client *gophercloud.ServiceClient, token string) string {
	return client.ServiceURL("tokens", token)
}
