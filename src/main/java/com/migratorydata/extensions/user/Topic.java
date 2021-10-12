package com.migratorydata.extensions.user;

public class Topic {

    private Key key = new Key();
    private Limit limit = new Limit(100, 5000);

    public Key getKey() {
        return key;
    }

    public void addKey(String secret) {
        key.addKey(secret);
    }

    public void deleteKey(String secret) {
        key.deleteKey(secret);
    }

    public void addLimit(Limit limit) {
        this.limit = limit;
    }

    public Limit getLimit() {
        return limit;
    }

    @Override
    public String toString() {
        return "Topic{" +
                "key=" + key +
                ", limit=" + limit +
                '}';
    }
}
