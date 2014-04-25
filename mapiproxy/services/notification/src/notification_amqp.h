#pragma once


bool broker_is_alive(struct context *);
bool broker_connect(struct context *);
bool broker_disconnect(struct context *);
bool broker_start_consumer(struct context *);
bool broker_consume(struct context *);
