package cn.com.wang.test.es.rest.client.ResultEntity;

import java.util.Map;

/**
 * Created by wang on 5:57 PM 2/7/17.
 * Version:
 * Description:
 */
public class CopyrightResult {
    private String _scroll_id;
    private int took;
    private boolean timed_out;
    private Map<String, Object> _shards;
    private CopyrightHits hits;

    public String get_scroll_id() {
        return _scroll_id;
    }

    public void set_scroll_id(String _scroll_id) {
        this._scroll_id = _scroll_id;
    }

    public int getTook() {
        return took;
    }

    public void setTook(int took) {
        this.took = took;
    }

    public boolean isTimed_out() {
        return timed_out;
    }

    public void setTimed_out(boolean timed_out) {
        this.timed_out = timed_out;
    }

    public Map<String, Object> get_shards() {
        return _shards;
    }

    public void set_shards(Map<String, Object> _shards) {
        this._shards = _shards;
    }

    public CopyrightHits getHits() {
        return hits;
    }

    public void setHits(CopyrightHits hits) {
        this.hits = hits;
    }

    public boolean isEmpty() {
        return hits.getHits().isEmpty();
    }
}
