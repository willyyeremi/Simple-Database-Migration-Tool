

def database_version_value(version: str) -> dict[str: str]:
    """
    Get the major, minor, patch value from the version of the database
    
    Args:
        version(string): the version value of the database
    
    Returns
        semantic_version_dict(dict): dictionary that contain major, minor, and patch value
    """
    import re
    pattern: str = r'^.*?(?=[\-\+])'
    match = re.search(pattern, version)
    if match:
        match =  match.group(0)
    else:
        match = version
    match = match.split(".")
    semantic_version_part: list[str] = ["major", "minor", "patch"]
    semantic_version_dict: dict[str: str] = {}
    for i in range(0, len(match)):
        semantic_version_dict[semantic_version_part[i]] = match[i]
    if "minor" not in semantic_version_dict.keys():
        semantic_version_dict["minor"] = "0"
    if "patch" not in semantic_version_dict.keys():
        semantic_version_dict["patch"] = "0"
    return semantic_version_dict

# Example usage:
input_string_1 = "abc+def+ghi"
input_string_2 = "10.2.3"

result_1 = database_version_value(input_string_1)
print(result_1)
result_2 = database_version_value(input_string_2)
print(result_2)

