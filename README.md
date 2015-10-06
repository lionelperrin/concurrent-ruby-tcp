# concurrent-ruby-tcp
This repository is a proof of concept which aims at extending concurrent-ruby with the ability to call remote functions.

## Objectives
The final objective is to get a seamless integration with concurrent-ruby and especially the Futures from concurrent-ruby-edge.
Since blocks/lambda can't be serialized, focus is given to the execution of remote functions/methods, like: `Concurrent.future(:remote_add, 1, 2, 3)` or `Concurrent.future(1, :+, 2)`.

## Status/Todo

The current implementation is quite simple:
It takes advantage of the existing Future. The only difference is that a remote executed future requires a socket connection. Given this socket, the parameters are serialized using Marshal. For consistency with concurrent-ruby, this implementation should be changed to take advantage of the `ExecutorService`.

## Example

One can try the current implementation with:
```
$> bundle install
$> bundle exec ruby example.rb
```
