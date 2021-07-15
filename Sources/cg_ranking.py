# -*- coding: utf-8 -*-
from typing import Optional

from pandas import DataFrame, concat, MultiIndex
from pickle import load
from numpy import argmax

from get_coins_infos import load_local_coins_infos


def get_categories_from_coins_infos(
    fn: str = "./data/coins_infos_list.pkl", update=True
):
    """
    Charge le fichier des infos et ne renvois que les catégories.
    si update est true, recharge  depuis le dossiers './data/Coins_infos
    """
    if update:
        categories = load_local_coins_infos("./data/Coins_infos").categories
    else:
        with open(fn, "br") as fd:
            categories = load(fd).categories

    categories.index.name == "coin_id"
    return categories


def get_categories_ranking(fn: str = "./data/coins_infos_list.pkl", categories=None):
    if categories is None:
        categories = get_categories_from_coins_infos(fn)

    df = DataFrame([y for x in categories.values for y in x], columns=["categorie"])

    return df.groupby("categorie").apply(len).sort_values(ascending=False)


def exfiltre_unpopulaire(_list, order):
    """ne garde de la liste que la catégorie la plus populaire"""
    if len(_list) == 1:
        return _list[0]
    if len(_list) == 0:
        return ""
    return _list[argmax([order[c] for c in _list])]


def add_most_populare_categories(market_cap: DataFrame) -> DataFrame:
    """Add to each coin_id a categorie.  If several are returned by api, get the most populare
    df is expected to be market_cap
    """
    assert market_cap.index.names == ["date", "coin_id"], f"{market_cap.index.names}"
    _market_cap = market_cap.copy()
    # getting categories from files save in coin_info folder
    categories = get_categories_from_coins_infos(update=True)

    categories_ranking = get_categories_ranking(categories=categories)

    # on ne garde que les catégorie les plus populaire lorsqu'il y en a plusieurs pour un coin
    def _exfiltre_unpopulaire(_list):
        return exfiltre_unpopulaire(_list, order=categories_ranking)

    categories_pop = categories.apply(_exfiltre_unpopulaire)

    # getting coins from market_cap
    # make it more robust
    coins = _market_cap.index.levels[1]

    for (i, coin) in enumerate(coins):
        print(f"{i}/{len(coins)}", end="\r")
        try:
            _market_cap.loc[(slice(None), coin), "categorie"] = categories_pop.loc[coin]
        except KeyError:
            pass
        except Exception as e:
            print(i, coin, end=" ")
            raise (e)
    return DataFrame(_market_cap)


def get_coin_ranking(
    data: Optional[DataFrame] = None, effectif=20, dropna: bool = False
) -> DataFrame:
    """
    Take an dataFrame with a timeindex,
    for each row order the columns and return them sorted associated with rank
    """
    if data is None:
        with open("./data/interpolate-uniform-df.pkl", "br") as fd:
            data = load(fd)

    # many assertion on _s name an index should be checked
    # we build the ranking dataFrame
    _ranking = {}
    for (date, market_cap) in data.T.items():
        _s = (
            market_cap.sort_values(ascending=False)
            .rank(ascending=False)
            .iloc[:effectif]
        )
        _s.name = "_rank"
        _market_cap_selection = data.loc[date, _s.index]
        _market_cap_selection.name = "market_cap"
        _market_cap_selection = _market_cap_selection.apply(to_float)
        _df = concat([_market_cap_selection, _s], axis=1)
        _df.index.name = "coin_id"
        _df.columns = MultiIndex.from_product(
            [[date], _df.columns], names=["date", "---"]
        )
        _ranking[date] = _df

    ranking = concat([df for (ts, df) in _ranking.items()], axis=1)
    ranking = ranking.unstack(1).unstack("---")
    if dropna:
        ranking = ranking.dropna()

    return add_most_populare_categories(ranking)


def to_float(x) -> float:
    try:
        return float(round(x))
    except ValueError:
        return float(0)


def main():
    """main"""
    with open("./data/interpolate-uniform-df.pkl", "br") as fd:
        data = load(fd)

    return get_coin_ranking(data)


if __name__ == "__main__":
    main()
