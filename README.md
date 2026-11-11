# MultiRegion PubSub
Proof of concept demonstrating Pub/Sub for a multi-region microservice architecture.

## Problem
Image we have a vending machine API. 
We can create a vending machine, restock its products, purchase a product, emit a payment through a webhook, or select a vending machine.
The purchase product API call will be entirely blocking, and needs to block in its entirety until a payment arrives.
At the end of the purchase product call, we select the product state to give the user the most up-to-date state (are we dispensing a product, is it successful, etc.).
However, we want to signal to the blocking goroutine in the purchase API call that a purchase is complete so it can return early since the common case is that a purchase comes quick and will not time out. To do that, we will use Redis Pub/Sub.
Since we are trying to improve the latency of this call (on average) and will still be selecting the correct state at the end of the call, Redis's "at most once delivery" is acceptable.

## Implementations

### Go + Redis
The Go implementation is a standard net/http web server that uses a fan-out Redis Pub/Sub implementation.
That is to say each Go node is aware of the Redis Pub/Sub nodes in each region, and will fan out messages to each region so they can be broadcasted to subscribers.

### Erlang
TODO

## TODO
- Deploy the Go implementation to AWS using Terraform
- Write the Erlang implementation
- Deploy the Erlang implementation to AWS using Terraform
- Benchmark the implementations to see which does better under high load