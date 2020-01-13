def reverse_list(input_list):
    """
    Reverses order of elements in list input_list.
    """
    return input_list[::-1]
    #return list

def count_digits(number):
    """
    Return count of digits
    """
    count = 0
    while(number > 0):
        count=count+1
        number = number//10
    return count