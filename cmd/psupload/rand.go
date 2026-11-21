package main

import crand "crypto/rand"

func randRead(b []byte) (int, error) { return crand.Read(b) }
