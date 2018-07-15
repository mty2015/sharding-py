import unittest
from shardingpy.api.config.base import load_sharding_rule_config_from_dict
from shardingpy.rule.base import ShardingRule
from . import rewrite_rule


class SQLRewriteEngineTest(unittest.TestCase):
    def setUp(self):
        sharding_rule_config = load_sharding_rule_config_from_dict(rewrite_rule.sharding_rule_config['sharding_rule'])
        self.sharding_rule = ShardingRule(sharding_rule_config,
                                          rewrite_rule.sharding_rule_config['data_sources'].keys())
        