package com.codect.writers;

import java.io.IOException;

import com.codect.common.Fields;

public class ApiWriter extends InMemoryWriter{
	@Override
    public void close() throws IOException {
        params.put(Fields.results, all);
    }
}