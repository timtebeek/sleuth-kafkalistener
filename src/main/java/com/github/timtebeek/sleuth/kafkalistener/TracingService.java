package com.github.timtebeek.sleuth.kafkalistener;

import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapInjectAdapter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class TracingService {
    private final Tracer tracer;

    public Map<String, String> getSpanHeaders() {
        Map<String, String> spanHeaders = new HashMap<>();
        tracer.inject(tracer.activeSpan().context(), Format.Builtin.TEXT_MAP, new TextMapInjectAdapter(spanHeaders));
        return spanHeaders;
    }
}