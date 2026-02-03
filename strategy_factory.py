from DataStrategy import GenericStrategy
from algo_config import TEST_ITEM_CONFIG


def get_strategy(test_item: str) -> GenericStrategy:
    cfg = TEST_ITEM_CONFIG.get(test_item)
    if not cfg:
        raise Exception(f"Unsupported test_item={test_item}")
    return GenericStrategy(cfg)


# def get_script(test_item: str) -> str:
#     cfg = TEST_ITEM_CONFIG[test_item]
#     script_path = f"/opt/algo/scripts/{cfg['script']}"
#     with open(script_path, "r", encoding="utf-8") as f:
#         return f.read()
