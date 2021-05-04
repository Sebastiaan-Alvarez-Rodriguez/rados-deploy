from enum import Enum

class Designation(Enum):
    OSD = 0,
    MON = 1,
    MGR = 2,
    MDS = 4

    @staticmethod
    def toint(designations):
        ans = 0
        for x in designations:
            ans |= x
        return ans

    @staticmethod
    def fromint(integer):
        if not isinstance(integer, int):
            integer = int(integer)
        return [x for x in Designation if x.value & integer != 0]