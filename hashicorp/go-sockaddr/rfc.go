package sockaddr

const ForwardingBlacklist = 4294967295
const ForwardingBlacklistRFC = "4294967295"

func IsRFC(rfcNum uint, sa SockAddr) bool {
	rfcNetMap := KnownRFCs()
	rfcNets, ok := rfcNetMap[rfcNum]
	if !ok {
		return false
	}

	var contained bool
	for _, rfcNet := range rfcNets {
		if rfcNet.Contains(sa) {
			contained = true
			break
		}
	}
	return contained
}

//
//
//
func KnownRFCs() map[uint]SockAddrs {

	return map[uint]SockAddrs{
		919: {

			MustIPv4Addr("255.255.255.255/32"), // [RFC1122], §7 Broadcast IP Addressing - Proposed Standards
		},
		1122: {

			MustIPv4Addr("0.0.0.0/8"),   // [RFC1122], §3.2.1.3
			MustIPv4Addr("127.0.0.0/8"), // [RFC1122], §3.2.1.3
		},
		1112: {

			MustIPv4Addr("224.0.0.0/4"), // [RFC1112], §4 Host Group Addresses
		},
		1918: {

			MustIPv4Addr("10.0.0.0/8"),
			MustIPv4Addr("172.16.0.0/12"),
			MustIPv4Addr("192.168.0.0/16"),
		},
		2544: {

			MustIPv4Addr("198.18.0.0/15"),
		},
		2765: {

			MustIPv6Addr("0:0:0:0:0:ffff:0:0/96"),
		},
		2928: {

			MustIPv6Addr("2001::/16"), // Superblock

		},
		3056: { // 6to4 address

			MustIPv6Addr("2002::/16"),
		},
		3068: {

			MustIPv4Addr("192.88.99.0/24"),

			//

			MustIPv6Addr("2002:c058:6301::/120"),
		},
		3171: {

			MustIPv4Addr("224.0.0.0/4"),
		},
		3330: {

			MustIPv4Addr("0.0.0.0/8"),

			MustIPv4Addr("10.0.0.0/8"),

			MustIPv4Addr("127.0.0.0/8"),

			MustIPv4Addr("169.254.0.0/16"),

			MustIPv4Addr("172.16.0.0/12"),

			MustIPv4Addr("192.0.2.0/24"),

			MustIPv4Addr("192.88.99.0/24"),

			MustIPv4Addr("192.168.0.0/16"),

			MustIPv4Addr("198.18.0.0/15"),

			MustIPv4Addr("224.0.0.0/4"),

			MustIPv4Addr("240.0.0.0/4"),
		},
		3849: {

			MustIPv6Addr("2001:db8::/32"), // [RFC3849], §4 IANA Considerations
		},
		3927: {

			MustIPv4Addr("169.254.0.0/16"), // [RFC3927], §2.1 Link-Local Address Selection
		},
		4038: {

			MustIPv6Addr("0:0:0:0:0:ffff::/96"),
		},
		4193: {

			MustIPv6Addr("fc00::/7"),
		},
		4291: {

			MustIPv6Addr("::/128"),

			MustIPv6Addr("::1/128"),

			MustIPv6Addr("::/96"),

			MustIPv6Addr("::ffff:0:0/96"),

			MustIPv6Addr("fe80::/10"),

			MustIPv6Addr("fec0::/10"),

			MustIPv6Addr("ff00::/8"),

			//

			//

			//

		},
		4380: {

			MustIPv6Addr("2001:0000::/32"),
		},
		4773: {

			MustIPv6Addr("2001:0000::/23"), // IANA
		},
		4843: {

			MustIPv6Addr("2001:10::/28"), // [RFC4843], §7 IANA Considerations
		},
		5180: {

			MustIPv6Addr("2001:0200::/48"), // [RFC5180], §8 IANA Considerations
		},
		5735: {

			MustIPv4Addr("192.0.2.0/24"),    // TEST-NET-1
			MustIPv4Addr("198.51.100.0/24"), // TEST-NET-2
			MustIPv4Addr("203.0.113.0/24"),  // TEST-NET-3
			MustIPv4Addr("198.18.0.0/15"),   // Benchmarks
		},
		5737: {

			MustIPv4Addr("192.0.2.0/24"),    // TEST-NET-1
			MustIPv4Addr("198.51.100.0/24"), // TEST-NET-2
			MustIPv4Addr("203.0.113.0/24"),  // TEST-NET-3
		},
		6052: {

			MustIPv6Addr("64:ff9b::/96"), // [RFC6052], §2.1. Well-Known Prefix
		},
		6333: {

			MustIPv4Addr("192.0.0.0/29"), // [RFC6333], §5.7 Well-Known IPv4 Address
		},
		6598: {

			MustIPv4Addr("100.64.0.0/10"),
		},
		6666: {

			MustIPv6Addr("0100::/64"),
		},
		6890: {

			/*
			   The IPv4 and IPv6 Special-Purpose Address Registries maintain the
			   following information regarding each entry:

			   o  Address Block - A block of IPv4 or IPv6 addresses that has been
			      registered for a special purpose.

			   o  Name - A descriptive name for the special-purpose address block.

			   o  RFC - The RFC through which the special-purpose address block was
			      requested.

			   o  Allocation Date - The date upon which the special-purpose address
			      block was allocated.

			   o  Termination Date - The date upon which the allocation is to be
			      terminated.  This field is applicable for limited-use allocations
			      only.

			   o  Source - A boolean value indicating whether an address from the
			      allocated special-purpose address block is valid when used as the
			      source address of an IP datagram that transits two devices.

			   o  Destination - A boolean value indicating whether an address from
			      the allocated special-purpose address block is valid when used as
			      the destination address of an IP datagram that transits two
			      devices.

			   o  Forwardable - A boolean value indicating whether a router may
			      forward an IP datagram whose destination address is drawn from the
			      allocated special-purpose address block between external
			      interfaces.

			   o  Global - A boolean value indicating whether an IP datagram whose
			      destination address is drawn from the allocated special-purpose
			      address block is forwardable beyond a specified administrative
			      domain.

			   o  Reserved-by-Protocol - A boolean value indicating whether the
			      special-purpose address block is reserved by IP, itself.  This
			      value is "TRUE" if the RFC that created the special-purpose
			      address block requires all compliant IP implementations to behave
			      in a special way when processing packets either to or from
			      addresses contained by the address block.

			   If the value of "Destination" is FALSE, the values of "Forwardable"
			   and "Global" must also be false.
			*/

			/*+----------------------+----------------------------+
			* | Attribute            | Value                      |
			* +----------------------+----------------------------+
			* | Address Block        | 0.0.0.0/8                  |
			* | Name                 | "This host on this network"|
			* | RFC                  | [RFC1122], Section 3.2.1.3 |
			* | Allocation Date      | September 1981             |
			* | Termination Date     | N/A                        |
			* | Source               | True                       |
			* | Destination          | False                      |
			* | Forwardable          | False                      |
			* | Global               | False                      |
			* | Reserved-by-Protocol | True                       |
			* +----------------------+----------------------------+*/
			MustIPv4Addr("0.0.0.0/8"),

			/*+----------------------+---------------+
			* | Attribute            | Value         |
			* +----------------------+---------------+
			* | Address Block        | 10.0.0.0/8    |
			* | Name                 | Private-Use   |
			* | RFC                  | [RFC1918]     |
			* | Allocation Date      | February 1996 |
			* | Termination Date     | N/A           |
			* | Source               | True          |
			* | Destination          | True          |
			* | Forwardable          | True          |
			* | Global               | False         |
			* | Reserved-by-Protocol | False         |
			* +----------------------+---------------+ */
			MustIPv4Addr("10.0.0.0/8"),

			/*+----------------------+----------------------+
			  | Attribute            | Value                |
			  +----------------------+----------------------+
			  | Address Block        | 100.64.0.0/10        |
			  | Name                 | Shared Address Space |
			  | RFC                  | [RFC6598]            |
			  | Allocation Date      | April 2012           |
			  | Termination Date     | N/A                  |
			  | Source               | True                 |
			  | Destination          | True                 |
			  | Forwardable          | True                 |
			  | Global               | False                |
			  | Reserved-by-Protocol | False                |
			  +----------------------+----------------------+*/
			MustIPv4Addr("100.64.0.0/10"),

			/*+----------------------+----------------------------+
			  | Attribute            | Value                      |
			  +----------------------+----------------------------+
			  | Address Block        | 127.0.0.0/8                |
			  | Name                 | Loopback                   |
			  | RFC                  | [RFC1122], Section 3.2.1.3 |
			  | Allocation Date      | September 1981             |
			  | Termination Date     | N/A                        |
			  | Source               | False [1]                  |
			  | Destination          | False [1]                  |
			  | Forwardable          | False [1]                  |
			  | Global               | False [1]                  |
			  | Reserved-by-Protocol | True                       |
			  +----------------------+----------------------------+*/

			MustIPv4Addr("127.0.0.0/8"),

			/*+----------------------+----------------+
			  | Attribute            | Value          |
			  +----------------------+----------------+
			  | Address Block        | 169.254.0.0/16 |
			  | Name                 | Link Local     |
			  | RFC                  | [RFC3927]      |
			  | Allocation Date      | May 2005       |
			  | Termination Date     | N/A            |
			  | Source               | True           |
			  | Destination          | True           |
			  | Forwardable          | False          |
			  | Global               | False          |
			  | Reserved-by-Protocol | True           |
			  +----------------------+----------------+*/
			MustIPv4Addr("169.254.0.0/16"),

			/*+----------------------+---------------+
			  | Attribute            | Value         |
			  +----------------------+---------------+
			  | Address Block        | 172.16.0.0/12 |
			  | Name                 | Private-Use   |
			  | RFC                  | [RFC1918]     |
			  | Allocation Date      | February 1996 |
			  | Termination Date     | N/A           |
			  | Source               | True          |
			  | Destination          | True          |
			  | Forwardable          | True          |
			  | Global               | False         |
			  | Reserved-by-Protocol | False         |
			  +----------------------+---------------+*/
			MustIPv4Addr("172.16.0.0/12"),

			/*+----------------------+---------------------------------+
			  | Attribute            | Value                           |
			  +----------------------+---------------------------------+
			  | Address Block        | 192.0.0.0/24 [2]                |
			  | Name                 | IETF Protocol Assignments       |
			  | RFC                  | Section 2.1 of this document    |
			  | Allocation Date      | January 2010                    |
			  | Termination Date     | N/A                             |
			  | Source               | False                           |
			  | Destination          | False                           |
			  | Forwardable          | False                           |
			  | Global               | False                           |
			  | Reserved-by-Protocol | False                           |
			  +----------------------+---------------------------------+*/

			MustIPv4Addr("192.0.0.0/24"),

			/*+----------------------+--------------------------------+
			  | Attribute            | Value                          |
			  +----------------------+--------------------------------+
			  | Address Block        | 192.0.0.0/29                   |
			  | Name                 | IPv4 Service Continuity Prefix |
			  | RFC                  | [RFC6333], [RFC7335]           |
			  | Allocation Date      | June 2011                      |
			  | Termination Date     | N/A                            |
			  | Source               | True                           |
			  | Destination          | True                           |
			  | Forwardable          | True                           |
			  | Global               | False                          |
			  | Reserved-by-Protocol | False                          |
			  +----------------------+--------------------------------+*/
			MustIPv4Addr("192.0.0.0/29"),

			/*+----------------------+----------------------------+
			  | Attribute            | Value                      |
			  +----------------------+----------------------------+
			  | Address Block        | 192.0.2.0/24               |
			  | Name                 | Documentation (TEST-NET-1) |
			  | RFC                  | [RFC5737]                  |
			  | Allocation Date      | January 2010               |
			  | Termination Date     | N/A                        |
			  | Source               | False                      |
			  | Destination          | False                      |
			  | Forwardable          | False                      |
			  | Global               | False                      |
			  | Reserved-by-Protocol | False                      |
			  +----------------------+----------------------------+*/
			MustIPv4Addr("192.0.2.0/24"),

			/*+----------------------+--------------------+
			  | Attribute            | Value              |
			  +----------------------+--------------------+
			  | Address Block        | 192.88.99.0/24     |
			  | Name                 | 6to4 Relay Anycast |
			  | RFC                  | [RFC3068]          |
			  | Allocation Date      | June 2001          |
			  | Termination Date     | N/A                |
			  | Source               | True               |
			  | Destination          | True               |
			  | Forwardable          | True               |
			  | Global               | True               |
			  | Reserved-by-Protocol | False              |
			  +----------------------+--------------------+*/
			MustIPv4Addr("192.88.99.0/24"),

			/*+----------------------+----------------+
			  | Attribute            | Value          |
			  +----------------------+----------------+
			  | Address Block        | 192.168.0.0/16 |
			  | Name                 | Private-Use    |
			  | RFC                  | [RFC1918]      |
			  | Allocation Date      | February 1996  |
			  | Termination Date     | N/A            |
			  | Source               | True           |
			  | Destination          | True           |
			  | Forwardable          | True           |
			  | Global               | False          |
			  | Reserved-by-Protocol | False          |
			  +----------------------+----------------+*/
			MustIPv4Addr("192.168.0.0/16"),

			/*+----------------------+---------------+
			  | Attribute            | Value         |
			  +----------------------+---------------+
			  | Address Block        | 198.18.0.0/15 |
			  | Name                 | Benchmarking  |
			  | RFC                  | [RFC2544]     |
			  | Allocation Date      | March 1999    |
			  | Termination Date     | N/A           |
			  | Source               | True          |
			  | Destination          | True          |
			  | Forwardable          | True          |
			  | Global               | False         |
			  | Reserved-by-Protocol | False         |
			  +----------------------+---------------+*/
			MustIPv4Addr("198.18.0.0/15"),

			/*+----------------------+----------------------------+
			  | Attribute            | Value                      |
			  +----------------------+----------------------------+
			  | Address Block        | 198.51.100.0/24            |
			  | Name                 | Documentation (TEST-NET-2) |
			  | RFC                  | [RFC5737]                  |
			  | Allocation Date      | January 2010               |
			  | Termination Date     | N/A                        |
			  | Source               | False                      |
			  | Destination          | False                      |
			  | Forwardable          | False                      |
			  | Global               | False                      |
			  | Reserved-by-Protocol | False                      |
			  +----------------------+----------------------------+*/
			MustIPv4Addr("198.51.100.0/24"),

			/*+----------------------+----------------------------+
			  | Attribute            | Value                      |
			  +----------------------+----------------------------+
			  | Address Block        | 203.0.113.0/24             |
			  | Name                 | Documentation (TEST-NET-3) |
			  | RFC                  | [RFC5737]                  |
			  | Allocation Date      | January 2010               |
			  | Termination Date     | N/A                        |
			  | Source               | False                      |
			  | Destination          | False                      |
			  | Forwardable          | False                      |
			  | Global               | False                      |
			  | Reserved-by-Protocol | False                      |
			  +----------------------+----------------------------+*/
			MustIPv4Addr("203.0.113.0/24"),

			/*+----------------------+----------------------+
			  | Attribute            | Value                |
			  +----------------------+----------------------+
			  | Address Block        | 240.0.0.0/4          |
			  | Name                 | Reserved             |
			  | RFC                  | [RFC1112], Section 4 |
			  | Allocation Date      | August 1989          |
			  | Termination Date     | N/A                  |
			  | Source               | False                |
			  | Destination          | False                |
			  | Forwardable          | False                |
			  | Global               | False                |
			  | Reserved-by-Protocol | True                 |
			  +----------------------+----------------------+*/
			MustIPv4Addr("240.0.0.0/4"),

			/*+----------------------+----------------------+
			  | Attribute            | Value                |
			  +----------------------+----------------------+
			  | Address Block        | 255.255.255.255/32   |
			  | Name                 | Limited Broadcast    |
			  | RFC                  | [RFC0919], Section 7 |
			  | Allocation Date      | October 1984         |
			  | Termination Date     | N/A                  |
			  | Source               | False                |
			  | Destination          | True                 |
			  | Forwardable          | False                |
			  | Global               | False                |
			  | Reserved-by-Protocol | False                |
			  +----------------------+----------------------+*/
			MustIPv4Addr("255.255.255.255/32"),

			/*+----------------------+------------------+
			  | Attribute            | Value            |
			  +----------------------+------------------+
			  | Address Block        | ::1/128          |
			  | Name                 | Loopback Address |
			  | RFC                  | [RFC4291]        |
			  | Allocation Date      | February 2006    |
			  | Termination Date     | N/A              |
			  | Source               | False            |
			  | Destination          | False            |
			  | Forwardable          | False            |
			  | Global               | False            |
			  | Reserved-by-Protocol | True             |
			  +----------------------+------------------+*/
			MustIPv6Addr("::1/128"),

			/*+----------------------+---------------------+
			  | Attribute            | Value               |
			  +----------------------+---------------------+
			  | Address Block        | ::/128              |
			  | Name                 | Unspecified Address |
			  | RFC                  | [RFC4291]           |
			  | Allocation Date      | February 2006       |
			  | Termination Date     | N/A                 |
			  | Source               | True                |
			  | Destination          | False               |
			  | Forwardable          | False               |
			  | Global               | False               |
			  | Reserved-by-Protocol | True                |
			  +----------------------+---------------------+*/
			MustIPv6Addr("::/128"),

			/*+----------------------+---------------------+
			  | Attribute            | Value               |
			  +----------------------+---------------------+
			  | Address Block        | 64:ff9b::/96        |
			  | Name                 | IPv4-IPv6 Translat. |
			  | RFC                  | [RFC6052]           |
			  | Allocation Date      | October 2010        |
			  | Termination Date     | N/A                 |
			  | Source               | True                |
			  | Destination          | True                |
			  | Forwardable          | True                |
			  | Global               | True                |
			  | Reserved-by-Protocol | False               |
			  +----------------------+---------------------+*/
			MustIPv6Addr("64:ff9b::/96"),

			/*+----------------------+---------------------+
			  | Attribute            | Value               |
			  +----------------------+---------------------+
			  | Address Block        | ::ffff:0:0/96       |
			  | Name                 | IPv4-mapped Address |
			  | RFC                  | [RFC4291]           |
			  | Allocation Date      | February 2006       |
			  | Termination Date     | N/A                 |
			  | Source               | False               |
			  | Destination          | False               |
			  | Forwardable          | False               |
			  | Global               | False               |
			  | Reserved-by-Protocol | True                |
			  +----------------------+---------------------+*/
			MustIPv6Addr("::ffff:0:0/96"),

			/*+----------------------+----------------------------+
			  | Attribute            | Value                      |
			  +----------------------+----------------------------+
			  | Address Block        | 100::/64                   |
			  | Name                 | Discard-Only Address Block |
			  | RFC                  | [RFC6666]                  |
			  | Allocation Date      | June 2012                  |
			  | Termination Date     | N/A                        |
			  | Source               | True                       |
			  | Destination          | True                       |
			  | Forwardable          | True                       |
			  | Global               | False                      |
			  | Reserved-by-Protocol | False                      |
			  +----------------------+----------------------------+*/
			MustIPv6Addr("100::/64"),

			/*+----------------------+---------------------------+
			  | Attribute            | Value                     |
			  +----------------------+---------------------------+
			  | Address Block        | 2001::/23                 |
			  | Name                 | IETF Protocol Assignments |
			  | RFC                  | [RFC2928]                 |
			  | Allocation Date      | September 2000            |
			  | Termination Date     | N/A                       |
			  | Source               | False[1]                  |
			  | Destination          | False[1]                  |
			  | Forwardable          | False[1]                  |
			  | Global               | False[1]                  |
			  | Reserved-by-Protocol | False                     |
			  +----------------------+---------------------------+*/

			MustIPv6Addr("2001::/16"),

			/*+----------------------+----------------+
			  | Attribute            | Value          |
			  +----------------------+----------------+
			  | Address Block        | 2001::/32      |
			  | Name                 | TEREDO         |
			  | RFC                  | [RFC4380]      |
			  | Allocation Date      | January 2006   |
			  | Termination Date     | N/A            |
			  | Source               | True           |
			  | Destination          | True           |
			  | Forwardable          | True           |
			  | Global               | False          |
			  | Reserved-by-Protocol | False          |
			  +----------------------+----------------+*/

			//

			/*+----------------------+----------------+
			  | Attribute            | Value          |
			  +----------------------+----------------+
			  | Address Block        | 2001:2::/48    |
			  | Name                 | Benchmarking   |
			  | RFC                  | [RFC5180]      |
			  | Allocation Date      | April 2008     |
			  | Termination Date     | N/A            |
			  | Source               | True           |
			  | Destination          | True           |
			  | Forwardable          | True           |
			  | Global               | False          |
			  | Reserved-by-Protocol | False          |
			  +----------------------+----------------+*/

			//

			/*+----------------------+---------------+
			  | Attribute            | Value         |
			  +----------------------+---------------+
			  | Address Block        | 2001:db8::/32 |
			  | Name                 | Documentation |
			  | RFC                  | [RFC3849]     |
			  | Allocation Date      | July 2004     |
			  | Termination Date     | N/A           |
			  | Source               | False         |
			  | Destination          | False         |
			  | Forwardable          | False         |
			  | Global               | False         |
			  | Reserved-by-Protocol | False         |
			  +----------------------+---------------+*/

			//

			/*+----------------------+--------------+
			  | Attribute            | Value        |
			  +----------------------+--------------+
			  | Address Block        | 2001:10::/28 |
			  | Name                 | ORCHID       |
			  | RFC                  | [RFC4843]    |
			  | Allocation Date      | March 2007   |
			  | Termination Date     | March 2014   |
			  | Source               | False        |
			  | Destination          | False        |
			  | Forwardable          | False        |
			  | Global               | False        |
			  | Reserved-by-Protocol | False        |
			  +----------------------+--------------+*/

			//

			/*+----------------------+---------------+
			  | Attribute            | Value         |
			  +----------------------+---------------+
			  | Address Block        | 2002::/16 [2] |
			  | Name                 | 6to4          |
			  | RFC                  | [RFC3056]     |
			  | Allocation Date      | February 2001 |
			  | Termination Date     | N/A           |
			  | Source               | True          |
			  | Destination          | True          |
			  | Forwardable          | True          |
			  | Global               | N/A [2]       |
			  | Reserved-by-Protocol | False         |
			  +----------------------+---------------+*/

			MustIPv6Addr("2002::/16"),

			/*+----------------------+--------------+
			  | Attribute            | Value        |
			  +----------------------+--------------+
			  | Address Block        | fc00::/7     |
			  | Name                 | Unique-Local |
			  | RFC                  | [RFC4193]    |
			  | Allocation Date      | October 2005 |
			  | Termination Date     | N/A          |
			  | Source               | True         |
			  | Destination          | True         |
			  | Forwardable          | True         |
			  | Global               | False        |
			  | Reserved-by-Protocol | False        |
			  +----------------------+--------------+*/
			MustIPv6Addr("fc00::/7"),

			/*+----------------------+-----------------------+
			  | Attribute            | Value                 |
			  +----------------------+-----------------------+
			  | Address Block        | fe80::/10             |
			  | Name                 | Linked-Scoped Unicast |
			  | RFC                  | [RFC4291]             |
			  | Allocation Date      | February 2006         |
			  | Termination Date     | N/A                   |
			  | Source               | True                  |
			  | Destination          | True                  |
			  | Forwardable          | False                 |
			  | Global               | False                 |
			  | Reserved-by-Protocol | True                  |
			  +----------------------+-----------------------+*/
			MustIPv6Addr("fe80::/10"),
		},
		7335: {

			MustIPv4Addr("192.0.0.0/29"), // [RFC7335], §6 IANA Considerations
		},
		ForwardingBlacklist: { // Pseudo-RFC

			//

			MustIPv4Addr("0.0.0.0/8"),
			MustIPv4Addr("127.0.0.0/8"),
			MustIPv4Addr("169.254.0.0/16"),
			MustIPv4Addr("192.0.0.0/24"),
			MustIPv4Addr("192.0.2.0/24"),
			MustIPv4Addr("198.51.100.0/24"),
			MustIPv4Addr("203.0.113.0/24"),
			MustIPv4Addr("240.0.0.0/4"),
			MustIPv4Addr("255.255.255.255/32"),
			MustIPv6Addr("::1/128"),
			MustIPv6Addr("::/128"),
			MustIPv6Addr("::ffff:0:0/96"),

			MustIPv6Addr("2001:db8::/32"),
			MustIPv6Addr("2001:10::/28"),
			MustIPv6Addr("fe80::/10"),
		},
	}
}

func VisitAllRFCs(fn func(rfcNum uint, sockaddrs SockAddrs)) {
	rfcNetMap := KnownRFCs()

	rfcBlacklist := map[uint]struct{}{
		ForwardingBlacklist: {},
	}

	for rfcNum, sas := range rfcNetMap {
		if _, found := rfcBlacklist[rfcNum]; !found {
			fn(rfcNum, sas)
		}
	}
}
