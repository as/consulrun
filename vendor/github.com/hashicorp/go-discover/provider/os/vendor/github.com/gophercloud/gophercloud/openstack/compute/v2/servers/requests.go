package servers

import (
	"encoding/base64"
	"encoding/json"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/flavors"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/images"
	"github.com/gophercloud/gophercloud/pagination"
)

type ListOptsBuilder interface {
	ToServerListQuery() (string, error)
}

type ListOpts struct {
	ChangesSince string `q:"changes-since"`

	Image string `q:"image"`

	Flavor string `q:"flavor"`

	Name string `q:"name"`

	Status string `q:"status"`

	Host string `q:"host"`

	Marker string `q:"marker"`

	Limit int `q:"limit"`

	AllTenants bool `q:"all_tenants"`

	TenantID string `q:"tenant_id"`
}

func (opts ListOpts) ToServerListQuery() (string, error) {
	q, err := gophercloud.BuildQueryString(opts)
	return q.String(), err
}

func List(client *gophercloud.ServiceClient, opts ListOptsBuilder) pagination.Pager {
	url := listDetailURL(client)
	if opts != nil {
		query, err := opts.ToServerListQuery()
		if err != nil {
			return pagination.Pager{Err: err}
		}
		url += query
	}
	return pagination.NewPager(client, url, func(r pagination.PageResult) pagination.Page {
		return ServerPage{pagination.LinkedPageBase{PageResult: r}}
	})
}

type CreateOptsBuilder interface {
	ToServerCreateMap() (map[string]interface{}, error)
}

type Network struct {
	UUID string

	Port string

	FixedIP string
}

type Personality []*File

// File is used within CreateOpts and RebuildOpts to inject a file into the
// File implements the json.Marshaler interface, so when a Create or Rebuild
type File struct {
	Path string

	Contents []byte
}

func (f *File) MarshalJSON() ([]byte, error) {
	file := struct {
		Path     string `json:"path"`
		Contents string `json:"contents"`
	}{
		Path:     f.Path,
		Contents: base64.StdEncoding.EncodeToString(f.Contents),
	}
	return json.Marshal(file)
}

type CreateOpts struct {
	Name string `json:"name" required:"true"`

	ImageRef string `json:"imageRef"`

	ImageName string `json:"-"`

	FlavorRef string `json:"flavorRef"`

	FlavorName string `json:"-"`

	SecurityGroups []string `json:"-"`

	UserData []byte `json:"-"`

	AvailabilityZone string `json:"availability_zone,omitempty"`

	Networks []Network `json:"-"`

	Metadata map[string]string `json:"metadata,omitempty"`

	Personality Personality `json:"personality,omitempty"`

	ConfigDrive *bool `json:"config_drive,omitempty"`

	AdminPass string `json:"adminPass,omitempty"`

	AccessIPv4 string `json:"accessIPv4,omitempty"`

	AccessIPv6 string `json:"accessIPv6,omitempty"`

	ServiceClient *gophercloud.ServiceClient `json:"-"`
}

func (opts CreateOpts) ToServerCreateMap() (map[string]interface{}, error) {
	sc := opts.ServiceClient
	opts.ServiceClient = nil
	b, err := gophercloud.BuildRequestBody(opts, "")
	if err != nil {
		return nil, err
	}

	if opts.UserData != nil {
		var userData string
		if _, err := base64.StdEncoding.DecodeString(string(opts.UserData)); err != nil {
			userData = base64.StdEncoding.EncodeToString(opts.UserData)
		} else {
			userData = string(opts.UserData)
		}
		b["user_data"] = &userData
	}

	if len(opts.SecurityGroups) > 0 {
		securityGroups := make([]map[string]interface{}, len(opts.SecurityGroups))
		for i, groupName := range opts.SecurityGroups {
			securityGroups[i] = map[string]interface{}{"name": groupName}
		}
		b["security_groups"] = securityGroups
	}

	if len(opts.Networks) > 0 {
		networks := make([]map[string]interface{}, len(opts.Networks))
		for i, net := range opts.Networks {
			networks[i] = make(map[string]interface{})
			if net.UUID != "" {
				networks[i]["uuid"] = net.UUID
			}
			if net.Port != "" {
				networks[i]["port"] = net.Port
			}
			if net.FixedIP != "" {
				networks[i]["fixed_ip"] = net.FixedIP
			}
		}
		b["networks"] = networks
	}

	if opts.ImageRef == "" {
		if opts.ImageName != "" {
			if sc == nil {
				err := ErrNoClientProvidedForIDByName{}
				err.Argument = "ServiceClient"
				return nil, err
			}
			imageID, err := images.IDFromName(sc, opts.ImageName)
			if err != nil {
				return nil, err
			}
			b["imageRef"] = imageID
		}
	}

	if opts.FlavorRef == "" {
		if opts.FlavorName == "" {
			err := ErrNeitherFlavorIDNorFlavorNameProvided{}
			err.Argument = "FlavorRef/FlavorName"
			return nil, err
		}
		if sc == nil {
			err := ErrNoClientProvidedForIDByName{}
			err.Argument = "ServiceClient"
			return nil, err
		}
		flavorID, err := flavors.IDFromName(sc, opts.FlavorName)
		if err != nil {
			return nil, err
		}
		b["flavorRef"] = flavorID
	}

	return map[string]interface{}{"server": b}, nil
}

func Create(client *gophercloud.ServiceClient, opts CreateOptsBuilder) (r CreateResult) {
	reqBody, err := opts.ToServerCreateMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Post(listURL(client), reqBody, &r.Body, nil)
	return
}

func Delete(client *gophercloud.ServiceClient, id string) (r DeleteResult) {
	_, r.Err = client.Delete(deleteURL(client, id), nil)
	return
}

func ForceDelete(client *gophercloud.ServiceClient, id string) (r ActionResult) {
	_, r.Err = client.Post(actionURL(client, id), map[string]interface{}{"forceDelete": ""}, nil, nil)
	return
}

func Get(client *gophercloud.ServiceClient, id string) (r GetResult) {
	_, r.Err = client.Get(getURL(client, id), &r.Body, &gophercloud.RequestOpts{
		OkCodes: []int{200, 203},
	})
	return
}

type UpdateOptsBuilder interface {
	ToServerUpdateMap() (map[string]interface{}, error)
}

type UpdateOpts struct {
	Name string `json:"name,omitempty"`

	AccessIPv4 string `json:"accessIPv4,omitempty"`

	AccessIPv6 string `json:"accessIPv6,omitempty"`
}

func (opts UpdateOpts) ToServerUpdateMap() (map[string]interface{}, error) {
	return gophercloud.BuildRequestBody(opts, "server")
}

func Update(client *gophercloud.ServiceClient, id string, opts UpdateOptsBuilder) (r UpdateResult) {
	b, err := opts.ToServerUpdateMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Put(updateURL(client, id), b, &r.Body, &gophercloud.RequestOpts{
		OkCodes: []int{200},
	})
	return
}

func ChangeAdminPassword(client *gophercloud.ServiceClient, id, newPassword string) (r ActionResult) {
	b := map[string]interface{}{
		"changePassword": map[string]string{
			"adminPass": newPassword,
		},
	}
	_, r.Err = client.Post(actionURL(client, id), b, nil, nil)
	return
}

type RebootMethod string

const (
	SoftReboot RebootMethod = "SOFT"
	HardReboot RebootMethod = "HARD"
	OSReboot                = SoftReboot
	PowerCycle              = HardReboot
)

type RebootOptsBuilder interface {
	ToServerRebootMap() (map[string]interface{}, error)
}

type RebootOpts struct {
	Type RebootMethod `json:"type" required:"true"`
}

// ToServerRebootMap builds a body for the reboot request.
func (opts *RebootOpts) ToServerRebootMap() (map[string]interface{}, error) {
	return gophercloud.BuildRequestBody(opts, "reboot")
}

/*
	Reboot requests that a given server reboot.

	Two methods exist for rebooting a server:

	HardReboot (aka PowerCycle) starts the server instance by physically cutting
	power to the machine, or if a VM, terminating it at the hypervisor level.
	It's done. Caput. Full stop.
	Then, after a brief while, power is rtored or the VM instance restarted.

	SoftReboot (aka OSReboot) simply tells the OS to restart under its own
	procedure.
	E.g., in Linux, asking it to enter runlevel 6, or executing
	"sudo shutdown -r now", or by asking Windows to rtart the machine.
*/
func Reboot(client *gophercloud.ServiceClient, id string, opts RebootOptsBuilder) (r ActionResult) {
	b, err := opts.ToServerRebootMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Post(actionURL(client, id), b, nil, nil)
	return
}

// RebuildOptsBuilder allows extensions to provide additional parameters to the
// rebuild request.
type RebuildOptsBuilder interface {
	ToServerRebuildMap() (map[string]interface{}, error)
}

// RebuildOpts represents the configuration options used in a server rebuild
type RebuildOpts struct {
	AdminPass string `json:"adminPass,omitempty"`

	ImageID string `json:"imageRef"`

	ImageName string `json:"-"`

	Name string `json:"name,omitempty"`

	AccessIPv4 string `json:"accessIPv4,omitempty"`

	AccessIPv6 string `json:"accessIPv6,omitempty"`

	Metadata map[string]string `json:"metadata,omitempty"`

	Personality Personality `json:"personality,omitempty"`

	ServiceClient *gophercloud.ServiceClient `json:"-"`
}

// ToServerRebuildMap formats a RebuildOpts struct into a map for use in JSON
func (opts RebuildOpts) ToServerRebuildMap() (map[string]interface{}, error) {
	b, err := gophercloud.BuildRequestBody(opts, "")
	if err != nil {
		return nil, err
	}

	if opts.ImageID == "" {
		if opts.ImageName != "" {
			if opts.ServiceClient == nil {
				err := ErrNoClientProvidedForIDByName{}
				err.Argument = "ServiceClient"
				return nil, err
			}
			imageID, err := images.IDFromName(opts.ServiceClient, opts.ImageName)
			if err != nil {
				return nil, err
			}
			b["imageRef"] = imageID
		}
	}

	return map[string]interface{}{"rebuild": b}, nil
}

// Rebuild will reprovision the server according to the configuration options
// provided in the RebuildOpts struct.
func Rebuild(client *gophercloud.ServiceClient, id string, opts RebuildOptsBuilder) (r RebuildResult) {
	b, err := opts.ToServerRebuildMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Post(actionURL(client, id), b, &r.Body, nil)
	return
}

type ResizeOptsBuilder interface {
	ToServerResizeMap() (map[string]interface{}, error)
}

type ResizeOpts struct {
	FlavorRef string `json:"flavorRef" required:"true"`
}

func (opts ResizeOpts) ToServerResizeMap() (map[string]interface{}, error) {
	return gophercloud.BuildRequestBody(opts, "resize")
}

//
// Note that this implies rebuilding it.
//
// Unfortunately, one cannot pass rebuild parameters to the resize function.
func Resize(client *gophercloud.ServiceClient, id string, opts ResizeOptsBuilder) (r ActionResult) {
	b, err := opts.ToServerResizeMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Post(actionURL(client, id), b, nil, nil)
	return
}

func ConfirmResize(client *gophercloud.ServiceClient, id string) (r ActionResult) {
	_, r.Err = client.Post(actionURL(client, id), map[string]interface{}{"confirmResize": nil}, nil, &gophercloud.RequestOpts{
		OkCodes: []int{201, 202, 204},
	})
	return
}

func RevertResize(client *gophercloud.ServiceClient, id string) (r ActionResult) {
	_, r.Err = client.Post(actionURL(client, id), map[string]interface{}{"revertResize": nil}, nil, nil)
	return
}

type RescueOptsBuilder interface {
	ToServerRescueMap() (map[string]interface{}, error)
}

type RescueOpts struct {
	AdminPass string `json:"adminPass,omitempty"`
}

func (opts RescueOpts) ToServerRescueMap() (map[string]interface{}, error) {
	return gophercloud.BuildRequestBody(opts, "rescue")
}

func Rescue(client *gophercloud.ServiceClient, id string, opts RescueOptsBuilder) (r RescueResult) {
	b, err := opts.ToServerRescueMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Post(actionURL(client, id), b, &r.Body, &gophercloud.RequestOpts{
		OkCodes: []int{200},
	})
	return
}

type ResetMetadataOptsBuilder interface {
	ToMetadataResetMap() (map[string]interface{}, error)
}

type MetadataOpts map[string]string

func (opts MetadataOpts) ToMetadataResetMap() (map[string]interface{}, error) {
	return map[string]interface{}{"metadata": opts}, nil
}

func (opts MetadataOpts) ToMetadataUpdateMap() (map[string]interface{}, error) {
	return map[string]interface{}{"metadata": opts}, nil
}

func ResetMetadata(client *gophercloud.ServiceClient, id string, opts ResetMetadataOptsBuilder) (r ResetMetadataResult) {
	b, err := opts.ToMetadataResetMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Put(metadataURL(client, id), b, &r.Body, &gophercloud.RequestOpts{
		OkCodes: []int{200},
	})
	return
}

func Metadata(client *gophercloud.ServiceClient, id string) (r GetMetadataResult) {
	_, r.Err = client.Get(metadataURL(client, id), &r.Body, nil)
	return
}

type UpdateMetadataOptsBuilder interface {
	ToMetadataUpdateMap() (map[string]interface{}, error)
}

func UpdateMetadata(client *gophercloud.ServiceClient, id string, opts UpdateMetadataOptsBuilder) (r UpdateMetadataResult) {
	b, err := opts.ToMetadataUpdateMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Post(metadataURL(client, id), b, &r.Body, &gophercloud.RequestOpts{
		OkCodes: []int{200},
	})
	return
}

type MetadatumOptsBuilder interface {
	ToMetadatumCreateMap() (map[string]interface{}, string, error)
}

type MetadatumOpts map[string]string

func (opts MetadatumOpts) ToMetadatumCreateMap() (map[string]interface{}, string, error) {
	if len(opts) != 1 {
		err := gophercloud.ErrInvalidInput{}
		err.Argument = "servers.MetadatumOpts"
		err.Info = "Must have 1 and only 1 key-value pair"
		return nil, "", err
	}
	metadatum := map[string]interface{}{"meta": opts}
	var key string
	for k := range metadatum["meta"].(MetadatumOpts) {
		key = k
	}
	return metadatum, key, nil
}

func CreateMetadatum(client *gophercloud.ServiceClient, id string, opts MetadatumOptsBuilder) (r CreateMetadatumResult) {
	b, key, err := opts.ToMetadatumCreateMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Put(metadatumURL(client, id, key), b, &r.Body, &gophercloud.RequestOpts{
		OkCodes: []int{200},
	})
	return
}

func Metadatum(client *gophercloud.ServiceClient, id, key string) (r GetMetadatumResult) {
	_, r.Err = client.Get(metadatumURL(client, id, key), &r.Body, nil)
	return
}

func DeleteMetadatum(client *gophercloud.ServiceClient, id, key string) (r DeleteMetadatumResult) {
	_, r.Err = client.Delete(metadatumURL(client, id, key), nil)
	return
}

func ListAddresses(client *gophercloud.ServiceClient, id string) pagination.Pager {
	return pagination.NewPager(client, listAddressesURL(client, id), func(r pagination.PageResult) pagination.Page {
		return AddressPage{pagination.SinglePageBase(r)}
	})
}

func ListAddressesByNetwork(client *gophercloud.ServiceClient, id, network string) pagination.Pager {
	return pagination.NewPager(client, listAddressesByNetworkURL(client, id, network), func(r pagination.PageResult) pagination.Page {
		return NetworkAddressPage{pagination.SinglePageBase(r)}
	})
}

type CreateImageOptsBuilder interface {
	ToServerCreateImageMap() (map[string]interface{}, error)
}

type CreateImageOpts struct {
	Name string `json:"name" required:"true"`

	Metadata map[string]string `json:"metadata,omitempty"`
}

func (opts CreateImageOpts) ToServerCreateImageMap() (map[string]interface{}, error) {
	return gophercloud.BuildRequestBody(opts, "createImage")
}

func CreateImage(client *gophercloud.ServiceClient, id string, opts CreateImageOptsBuilder) (r CreateImageResult) {
	b, err := opts.ToServerCreateImageMap()
	if err != nil {
		r.Err = err
		return
	}
	resp, err := client.Post(actionURL(client, id), b, nil, &gophercloud.RequestOpts{
		OkCodes: []int{202},
	})
	r.Err = err
	r.Header = resp.Header
	return
}

func IDFromName(client *gophercloud.ServiceClient, name string) (string, error) {
	count := 0
	id := ""
	allPages, err := List(client, nil).AllPages()
	if err != nil {
		return "", err
	}

	all, err := ExtractServers(allPages)
	if err != nil {
		return "", err
	}

	for _, f := range all {
		if f.Name == name {
			count++
			id = f.ID
		}
	}

	switch count {
	case 0:
		return "", gophercloud.ErrResourceNotFound{Name: name, ResourceType: "server"}
	case 1:
		return id, nil
	default:
		return "", gophercloud.ErrMultipleResourcesFound{Name: name, Count: count, ResourceType: "server"}
	}
}

func GetPassword(client *gophercloud.ServiceClient, serverId string) (r GetPasswordResult) {
	_, r.Err = client.Get(passwordURL(client, serverId), &r.Body, nil)
	return
}
