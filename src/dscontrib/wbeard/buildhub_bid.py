from requests import post
import datetime as dt
import itertools as it

import pandas as pd
from pandas import DataFrame

uri = "https://buildhub.moz.tools/api/search"


def get_major(s):
    "str -> int"
    return int(s.split(".")[0])


def pull_build_id_docs(
    min_build_day="20180701", channel="beta", raw_json=False
):
    """
    Note, we're only taking win64, en-US, assuming that other build_ids will
    just be duplicates.
    """
    query = {
        "aggs": {
            "buildid": {
                "terms": {
                    "field": "build.id",
                    "size": 100000,
                    "order": {"_term": "desc"},
                },
                # "aggs": {"version": {"terms": {"field": "target.version"}}},
                "aggs": {
                    "version": {"terms": {"field": "target.version"}},
                    "pub_date": {"terms": {"field": "download.date"}},
                    "buildid": {"terms": {"field": "build.id"}},
                },
            }
        },
        "query": {
            "bool": {
                "filter": [
                    {"term": {"target.platform": "win64"}},
                    {"term": {"target.locale": "en-US"}},
                    {"term": {"target.channel": channel}},
                    {"term": {"source.product": "firefox"}},
                    {"range": {"build.id": {"gte": min_build_day}}},
                ]
            }
        },
        "size": 0,
    }
    resp = post(uri, json=query)
    json = resp.json()
    if raw_json:
        return json
    docs = json["aggregations"]["buildid"]["buckets"]
    return docs


def extract_triplets(
    doc, major_version=None, keep_rc=False, keep_release=False, agg=min
):
    """
    @major_version: int or (callable: str -> bool)
    [doc] = aggregations.buildid.buckets ->
        doc.version.buckets[].key
    From json results, return tuple of `buildids`, `pub_dates`, `versions`

    Some build_id's go to multiple versions (usually rc's or a major version)
    Some versions go to multiple build_id's.
    """

    def collect_results(field):
        return [res["key"] for res in doc[field]["buckets"]]

    def major_version_filt(v):
        if major_version is None:
            return True
        elif isinstance(major_version, int):
            return get_major(v) == major_version
        else:
            return major_version(v)

    buildids = collect_results("buildid")
    pub_dates = collect_results("pub_date")
    versions = [
        v
        for v in collect_results("version")
        if version_filter(v, keep_rc=keep_rc, keep_release=keep_release)
        and major_version_filt(v)
    ]
    if not versions:
        return None
    return agg(versions), agg(buildids), agg(pub_dates)


def version2build_ids(
    docs, major_version=None, keep_rc=False, keep_release=False
):
    version_build_ids = [
        extract_triplets(
            doc,
            major_version=major_version,
            keep_rc=keep_rc,
            keep_release=keep_release,
        )
        for doc in docs
    ]
    version_build_ids = filter(None, version_build_ids)
    return {
        version: [
            (_version, build_id, pub_date)
            for _version, build_id, pub_date in triplets
        ]
        for version, triplets in it.groupby(version_build_ids, lambda x: x[0])
    }


def version2build_id_str(
    docs, major_version=None, keep_rc=False, keep_release=False
):
    """
    Returns mapping of display version to 'sql' list of build_ids.
    E.g., {'70.0b3': "'20190902191027', '20190902160204', '20190902120346'"}
    """
    triplet_dct = version2build_ids(
        docs,
        major_version=major_version,
        keep_rc=keep_rc,
        keep_release=keep_release,
    )
    return {
        dvers: ", ".join(
            ["'{}'".format(bid) for _dvers, bid, pub_date in trips]
        )
        for dvers, trips in triplet_dct.items()
    }


def version2df(docs, major_version=None, keep_rc=False, keep_release=False):
    """
    Given docs from ---
    return DataFrame with columns `disp_vers`, `build_id`, `pub_date`.
    """
    triples = version2build_ids(
        docs,
        major_version=major_version,
        keep_rc=keep_rc,
        keep_release=keep_release,
    )
    df = (
        DataFrame(
            [trip for trips in triples.values() for trip in trips],
            columns=["disp_vers", "build_id", "pub_date"],
        )
        .assign(
            pub_date=lambda x: (x.pub_date // 1e3)
            .map(dt.datetime.utcfromtimestamp)
            .pipe(pd.to_datetime)
        )
        .sort_values(["pub_date"], ascending=True)
        .reset_index(drop=1)
    )

    return df


def version_filter(x, keep_rc=False, keep_release=False):
    if not keep_rc and "rc" in x:
        return False
    if not keep_release and ("b" not in x):
        return False
    return True


def test_version_filter():
    rc_vers = "65.0b6rc"
    assert version_filter(rc_vers, keep_rc=False, keep_release=False) is False
    assert version_filter(rc_vers, keep_rc=True, keep_release=False)

    rls_vers = "65.0"
    assert version_filter(rls_vers, keep_rc=False, keep_release=True)
    assert version_filter(rls_vers, keep_rc=False, keep_release=False) is False

    beta_vers = "65.0b6"
    assert version_filter(beta_vers, keep_rc=False, keep_release=True)
    assert version_filter(beta_vers, keep_rc=False, keep_release=False)


def months_ago(months=12):
    return (dt.date.today() - dt.timedelta(days=30 * months)).strftime("%Y%m%d")


def main(vers=67):
    # result_docs = pull_build_id_docs(min_build_day="20180701")
    result_docs = pull_build_id_docs(
        min_build_day=months_ago(12), channel="beta"
    )
    res = version2build_ids(
        result_docs, major_version=67, keep_rc=False, keep_release=False
    )
    print(res)


def main_release(vers=67):
    # result_docs = pull_build_id_docs(min_build_day="20180701")
    result_docs = pull_build_id_docs(
        min_build_day=months_ago(12), channel="release"
    )
    res = version2build_ids(
        result_docs, major_version=67, keep_rc=False, keep_release=True
    )
    print(res)


if __name__ == "__main__":
    main()

    print("\n\nrelease")
    main_release()
