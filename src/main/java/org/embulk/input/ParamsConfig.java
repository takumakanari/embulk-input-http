package org.embulk.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;

public class ParamsConfig {

    private final List<QueryConfig> queries;

    @JsonCreator
    public ParamsConfig(List<QueryConfig> queries) {
        this.queries = queries;
    }

    @JsonValue
    public List<QueryConfig> getQueries() {
        return queries;
    }

    public List<List<QueryConfig.Query>> expandQueries() {
        List<List<QueryConfig.Query>> base = new ArrayList<>(queries.size());
        for (QueryConfig p : queries) {
            base.add(p.expand());
        }

        int productSize = 1;
        int baseSize = base.size();
        for (int i = 0; i < baseSize; productSize *= base.get(i).size(), i++) ;

        List<List<QueryConfig.Query>> expands = new ArrayList<>(productSize);
        for (int i = 0; i < productSize; i++) {
            int j = 1;
            List<QueryConfig.Query> query = new ArrayList<>();
            for (List<QueryConfig.Query> list : base) {
                QueryConfig.Query pc = list.get((i / j) % list.size());
                query.add(pc);
                j *= list.size();
            }
            expands.add(query);
        }

        return expands;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ParamsConfig)) {
            return false;
        }
        ParamsConfig other = (ParamsConfig) obj;
        return Objects.equal(queries, other.queries);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(queries);
    }

}
