package org.embulk.input.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

import java.util.ArrayList;
import java.util.List;

public class PagerConfig {

    private final String fromParam;
    private final Optional<String> toParam;
    private final int start;
    private final int pages;
    private final int step;

    @JsonCreator
    public PagerConfig(@JsonProperty("from_param") String fromParam,
            @JsonProperty("to_param") Optional<String> toParam,
            @JsonProperty("start") Optional<Integer> start,
            @JsonProperty("pages") int pages,
            @JsonProperty("step") Optional<Integer> step) {
        this.fromParam = fromParam;
        this.toParam = toParam;
        this.start = start.or(0);
        this.pages = pages;
        this.step = step.or(1);
    }

    public List<List<QueryConfig.Query>> expand() {
        List<List<QueryConfig.Query>> queries = new ArrayList<>();
        int p = 1;
        int index = start;
        while (p <= pages) {
            List<QueryConfig.Query> one = new ArrayList<>();
            one.add(new QueryConfig.Query(fromParam, Integer.toString(index)));
            if (toParam.isPresent()) {
                int t = index + step - 1;
                one.add(new QueryConfig.Query(toParam.get(), Integer.toString(t)));
                index = t + 1;
            } else {
                index += step;
            }
            queries.add(one);
            p++;
        }
        return queries;
    }

    @JsonProperty("from_param")
    public String getFromParam() {
        return fromParam;
    }

    @JsonProperty("to_param")
    public Optional<String> getToParam() {
        return toParam;
    }

    @JsonProperty("start")
    public int getStart() {
        return start;
    }

    @JsonProperty("pages")
    public int getPages() {
        return pages;
    }

    @JsonProperty("step")
    public int getStep() {
        return step;
    }

    @Override
    public String toString() {
        return "PagerConfig{" +
                "fromParam='" + fromParam + '\'' +
                ", toParam=" + toParam +
                ", start=" + start +
                ", pages=" + pages +
                ", step=" + step +
                '}';
    }
}
