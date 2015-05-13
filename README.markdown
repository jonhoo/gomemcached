# gomemcached

This is a memcached binary protocol toolkit in [go][go].

It provides client and server functionality as well as a little sample
server showing how I might make a server if I valued purity over
performance.

## Server Design

<div>
  <img src="http://dustin.github.com/images/gomemcached.png"
       alt="overview" style="float: right"/>
</div>

The basic design can be seen in [gocache].  A [storage
server][storage] is run as a goroutine that receives a `MCRequest` on
a channel, and then issues an `MCResponse` to a channel contained
within the request.

Each connection is a separate goroutine, of course, and is responsible
for all IO for that connection until the connection drops or the
`dataServer` decides it's stupid and sends a fatal response back over
the channel.

There is currently no work at all in making the thing perform (there
are specific areas I know need work).  This is just my attempt to
learn the language somewhat.

## This fork

This fork attempts to avoid extraneous copies when transmitting objects
by matching the in-memory representation with the on-wire format. The
performance improvements are ~10x for transmits:

```
benchmark                          old ns/op     new ns/op     delta
BenchmarkEncodingRequest           194           19.2          -90.10%
BenchmarkEncodingRequest0CAS       186           19.5          -89.52%
BenchmarkEncodingRequest1Extra     190           19.5          -89.74%
BenchmarkReceiveRequest            212           241           +13.68%
BenchmarkReceiveRequestNoBuf       297           363           +22.22%
BenchmarkEncodingResponse          195           19.1          -90.21%
BenchmarkEncodingResponseLarge     28665         19.2          -99.93%
BenchmarkReceiveResponse           203           242           +19.21%
BenchmarkReceiveResponseNoBuf      296           357           +20.61%

benchmark                          old MB/s     new MB/s       speedup
BenchmarkEncodingRequest           205.21       2086.10        10.17x
BenchmarkEncodingRequest0CAS       214.14       2049.70        9.57x
BenchmarkEncodingRequest1Extra     215.02       2100.60        9.77x
BenchmarkReceiveRequest            193.09       169.65         0.88x
BenchmarkReceiveRequestNoBuf       137.96       112.87         0.82x
BenchmarkEncodingResponse          204.76       2092.48        10.22x
BenchmarkEncodingResponseLarge     858.43       1279381.59     1490.37x
BenchmarkReceiveResponse           201.10       168.88         0.84x
BenchmarkReceiveResponseNoBuf      138.07       114.75         0.83x
```

[go]: http://golang.org/
[gocache]: ../master/gocache/gocache.go
[storage]: ../master/gocache/mc_storage.go
