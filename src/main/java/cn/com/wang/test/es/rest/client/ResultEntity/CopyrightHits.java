package cn.com.wang.test.es.rest.client.ResultEntity;

import java.util.List;
import java.util.Map;

/**
 * Created by wang on 5:51 PM 2/7/17.
 * Version:
 * Description:
 */
public class CopyrightHits {
    private long total;
    private double max_score;
    private List<Hits> hits;

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public double getMax_score() {
        return max_score;
    }

    public void setMax_score(double max_score) {
        this.max_score = max_score;
    }

    public List<Hits> getHits() {
        return hits;
    }

    public void setHits(List<Hits> hits) {
        this.hits = hits;
    }

    public class Hits {
        private String _index;
        private String _type;
        private String _id;
        private double _score;
        private Map<String, Object> _source;

        public String get_index() {
            return _index;
        }

        public void set_index(String _index) {
            this._index = _index;
        }

        public String get_type() {
            return _type;
        }

        public void set_type(String _type) {
            this._type = _type;
        }

        public String get_id() {
            return _id;
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        public double get_score() {
            return _score;
        }

        public void set_score(double _score) {
            this._score = _score;
        }

        public Map<String, Object> get_source() {
            return _source;
        }

        public void set_source(Map<String, Object> _source) {
            this._source = _source;
        }
    }

}
