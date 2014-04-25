#pragma once


bool broker_is_alive(struct context *);
bool broker_connect(struct context *);
bool broker_disconnect(struct context *);
