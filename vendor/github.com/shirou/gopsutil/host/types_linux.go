// +build ignore

/*
Input to cgo -godefs.
*/

package host

/*
#include <sys/types.h>
#include <utmp.h>

enum {
       sizeofPtr = sizeof(void*),
};

*/
import "C"

const (
	sizeofPtr      = C.sizeofPtr
	sizeofShort    = C.sizeof_short
	sizeofInt      = C.sizeof_int
	sizeofLong     = C.sizeof_long
	sizeofLongLong = C.sizeof_longlong
	sizeOfUtmp     = C.sizeof_struct_utmp
)

type (
	_C_short     C.short
	_C_int       C.int
	_C_long      C.long
	_C_long_long C.longlong
)

type utmp C.struct_utmp
type exit_status C.struct_exit_status
type timeval C.struct_timeval
