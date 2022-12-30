# Rational

Keep thinks simple and stupid.

Adding a redis service makes thinks more complicated

This is a try to use plain python utilities to provide channels layer functionality without configuration

# State

Non-functional.
I have currently not much time and don't know if I can complete it

# Things to think over

It would maybe better to use RemoteManager.
The whole logic is tied to that the asgi server uses a multiprocessing friendly approach.
I am currently not familar with how robust the implementation is.
Maybe there should be a client server model, with the server automatically spawning (RemoteManager approach) and closing after no input/when the asgi server is shutdown
