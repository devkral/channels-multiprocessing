# Rational

Keep thinks simple and stupid.

Adding a redis service makes thinks more complicated

This is a try to use plain python utilities to provide channels layer functionality without configuration

# idea

we leverage multiprocessing for providing a channel layer. As multiprocessing doesn't play always nice with async
we use a per layer a Thread to serialize the internal requests.
Per default the default mp_context is used for creating the manager for multiprocessing synchronization
It may be set manually to "spawn" in case of an non python asgi server with multiple process workers

# State

simple tests passed, still problems with expiry (values of dict differ)

# TODO

-   documentation
-   investigate aioprocessing
