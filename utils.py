'''Utils.

Author:
    P. Polakovic
'''

def ffs(num):
    '''Returns first signed bit.'''
    if num == 0:
        return None

    i = 0
    while num % 2 == 0:
        i += 1
        num = num >> 1
    return i


def qalign(num):
    '''Aligns `n` on 8 bytes boundary.'''
    return (num & ~0x7) + 0x8 if num % 0x08 else num