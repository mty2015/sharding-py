import importlib


def load_class_for_name(name):
    module_path, cls_name = name.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, cls_name)
