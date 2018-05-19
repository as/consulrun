# Consulrun

	`go get github.com/as/consulrun/...`

## What is it?

	Consulrun is an in-memory test consul server. 
	It's almost identical to the consul testutil package, except it's self-contained. The consul binary does not have be on the system.

## What problem does it solve?

	Testing code that uses consul requires an environment where the program is available in the test environment. 
	Depending on what that environment is, you might not be able to easily acquire the binary. 
	This solves the problem of easily writing unit tests for consul without worrying about the binary.

## How does it work?

	I took consul, vendored the dependencies, and lifted the logic of the main function into `consulrun.Cmd`. 
	Calling that function runs the logic of running the `consul` command itself with the given arguments.
	
	The consul testutil package is also explicity copied to a subpackage in this repository. 
	It uses the `consulrun.Cmd` command to run a local server and run the test of the `testutil` logic in it.

## How do I use it?

	Download the repository 

	Replace the import paths 
	`"github.com/hashicorp/consul/testutil"`  -> `"github.com/as/consulrun/testutil"`  

## Is there anything I should know before using it?

	- Consul doesn't seem to have a way of stopping the command with cancellation
		- When you use a `testutil.NewServer` to initialize a server for test, that listener will run until the test program ends.
		- If you have a huge test that continuously creates and destroys new testutil servers stopping the test server won't work and you may exhaust your ephemeral ports.
		- It is arguable that if you are creating and destroying the regular consul testserver, you are already exhausting those ports because of lingering sockets
	- I deleted the hashicorp providers (Azure, etc) in the vendor tree

## Does this package coexist with the regular consul?

	There should be no impact on consul, everything is vendored.

## How large is this package, will it bloat my project?

	The entire tree (this package and the vendor dir) is approx. 10MB
	The original tree was around 100MB

## License?
	
	Public domain