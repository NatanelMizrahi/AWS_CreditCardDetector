#https://www.geeksforgeeks.org/luhn-algorithm/
# Python3 program to implement
# Luhn algorithm

# Returns true if given card
# number is valid
def checkLuhn(cardNo):
    nDigits = len(cardNo)
    nSum = 0
    isSecond = False

    for i in range(nDigits - 1, -1, -1):
        d = ord(cardNo[i]) - ord('0')

        if (isSecond == True):
            d = d * 2

        # We add two digits to handle
        # cases that make two digits after
        # doubling
        nSum += d // 10
        nSum += d % 10

        isSecond = not isSecond

    if (nSum % 10 == 0):
        return True
    else:
        return False



def nested_range(list_of_lists):
    min_val = +float('inf')
    max_val = -float('inf')
    Q = [list_of_lists]
    while Q:
        e = Q.pop()
        if type(e) == int:
            min_val = min(min_val, e)
            max_val = max(max_val, e)
        else:
            for element in e:
                Q.append(element)
    return min_val, max_val
