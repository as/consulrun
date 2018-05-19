# Consulrun

`go get github.com/as/consulrun/...`

## What is it?

- Consulrun is an in-memory test consul server. 
	- Almost the same as consul testutil, except it's self-contained. 
	- The consul binary does not have be on the system.
	- No dep/glide/vgo necessary
	- No go get necessary (other than this package)

## What problem does it solve?

- Easily writing unit tests for consul without worrying about the binary.

## How does it work?

The process was 

- Take consul
- Harvest all dependencies
- Remove false dependencies
- Extract main into a package function
- Compact
	
The consul testutil package is also explicity copied to a subpackage in this repository. 
It uses the `consulrun.Cmd` command to run a local server and run the test of the `testutil` logic in it.

- Add `NewQuietServer` to testutil

## How do I use it?

- Download the repository 
	`go get github.com/as/consulrun/...`
	
- Replace the import paths 
	`"github.com/hashicorp/consul/testutil"`  -> `"github.com/as/consulrun/testutil"`  

## Is there anything I should know before using it?

-Consul doesn't seem to have a way of stopping the command with cancellation
- When you use a `testutil.NewServer` to initialize a server for test, that listener will run until the test program ends.
	- If you have a huge test that continuously creates and destroys new testutil servers stopping the test server won't work.
	- E.g., a unit test with over ~60000 calls to `testutil.NewServer`
- I deleted the hashicorp providers (Azure, etc) in the vendor tree

## Does this package coexist with the regular consul?

-There should be no impact on consul, everything is vendored.

## How large is this package, will it bloat my project?

-The entire tree (this package and the vendor dir) is approx. 10MB
-The original tree was around 100MB

## License?
	
-Public domain
