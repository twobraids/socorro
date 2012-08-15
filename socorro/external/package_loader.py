import os

from configman.converters import class_converter, py_obj_to_str
from configman import RequiredConfig, Namespace



#------------------------------------------------------------------------------
def package_list_converter(package_list_str):
    """ """
    if isinstance(package_list_str, basestring):
        package_list = [x.strip() for x in package_list_str.split(',')]
    else:
        raise TypeError('must be derivative of a basestring')

    #==========================================================================
    class InnerPackageList(RequiredConfig):
        """
        """
        # we're dynamically creating a class here.  The following block of
        # code is actually adding class level attributes to this new class
        required_config = Namespace()  # 1st requirement for configman
        subordinate_namespace_names = []  # to help the programmer know
                                          # what Namespaces we added
        # for each package in the package list
        for a_package in package_list:
            # figure out the Namespace name
            namespace_name = os.path.basename(a_package.replace('.', '/'))
            subordinate_namespace_names.append(namespace_name)
            # create the new Namespacea
            required_config[namespace_name] = Namespace()
            # add the option for the class itself
            the_package = class_converter(a_package)
            try:
                required_config[namespace_name] = \
                  the_package.required_config
            except AttributeError:
                pass


        @classmethod
        def to_str(cls):
            """this method takes this inner class object and turns it back
            into the original string of package names.  This is used
            primarily as for the output of the 'help' option"""
            return package_list_str

    return InnerPackageList  # result of class_list_converter
