import module

credential_data, credential_dict = module.credential_get() 
module.credential_check(credential_data)
# module.credential_object_maker(credential_dict)

module.runner(credential_dict)