def equals_ignore_case(str1, str2):
    str1 = str1.lower() if str1 else None
    str2 = str2.lower() if str2 else None
    return str1 == str2

