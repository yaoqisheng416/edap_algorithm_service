# -*- coding: utf-8 -*-
from DataStrategy import GenericStrategy


def get_strategy(test_item: str):
    """
    test_item 只用于日志标识，不参与策略选择
    """
    return GenericStrategy()