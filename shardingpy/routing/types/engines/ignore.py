from shardingpy.routing.types.base import RoutingResult


class IgnoreRoutingEngine:
    def route(self):
        return RoutingResult()
