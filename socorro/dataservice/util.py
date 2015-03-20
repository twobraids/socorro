# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import contextlib

from configman import Namespace, RequiredConfig
from configman.converters import class_converter, py_obj_to_str

from socorro.webapi.webapiService import DataserviceWebServiceBase


#------------------------------------------------------------------------------
def string_to_list(input_str):
    return [x.strip() for x in input_str.split(',') if x.strip()]


#------------------------------------------------------------------------------
def classes_in_namespaces_converter(
    name_of_class_option='cls',
):
    """take a comma delimited  list of class names, convert each class name
    into an actual class as an option within a numbered namespace.  This
    function creates a closure over a new function.  That new function,
    in turn creates a class derived from RequiredConfig.  The inner function,
    'class_list_converter', populates the InnerClassList with a Namespace for
    each of the classes in the class list.  In addition, it puts the each class
    itself into the subordinate Namespace.  The requirement discovery mechanism
    of configman then reads the InnerClassList's required config, pulling in
    the namespaces and associated classes within.

    For example, if we have a class list like this: "Alpha, Beta", then this
    converter will add the following Namespaces and options to the
    configuration:

        "Alpha" - the subordinate Namespace for Alpha
        "Alpha.cls" - the option containing the class Alpha itself
        "Beta" - the subordinate Namespace for Beta
        "Beta.cls" - the option containing the class Beta itself

    Optionally, the 'class_list_converter' inner function can embue the
    InnerClassList's subordinate namespaces with aggregates that will
    instantiate classes from the class list.  This is a convenience to the
    programmer who would otherwise have to know ahead of time what the
    namespace names were so that the classes could be instantiated within the
    context of the correct namespace.  Remember the user could completely
    change the list of classes at run time, so prediction could be difficult.

        "Alpha" - the subordinate Namespace for Alpha
        "Alpha.cls" - the option containing the class Alpha itself
        "Alpha.cls_instance" - an instance of the class Alpha
        "Beta" - the subordinate Namespace for Beta
        "Beta.cls" - the option containing the class Beta itself
        "Beta.cls_instance" - an instance of the class Beta

    parameters:
        class_option_name - the name to be used for the class option within
                            the nested namespace.  By default, it will choose:
                            "classname.class", "classname2.class", etc.
        instantiate_classes - a boolean to determine if there should be an
                              aggregator added to each namespace that
                              instantiates each class.  If True, then each
                              Namespace will contain elements for the class, as
                              well as an aggregator that will instantiate the
                              class.
                              """

    #--------------------------------------------------------------------------
    def class_list_converter(class_list_str):
        """This function becomes the actual converter used by configman to
        take a string and convert it into the nested sequence of Namespaces,
        one for each class in the list.  It does this by creating a proxy
        class stuffed with its own 'required_config' that's dynamically
        generated."""
        if isinstance(class_list_str, basestring):
            class_list = [x.strip() for x in class_list_str.split(',')]
            if class_list == ['']:
                class_list = []
        else:
            raise TypeError('must be derivative of a basestring')

        #======================================================================
        class InnerClassList(RequiredConfig):
            """This nested class is a proxy list for the classes.  It collects
            all the config requirements for the listed classes and places them
            each into their own Namespace.
            """
            # we're dynamically creating a class here.  The following block of
            # code is actually adding class level attributes to this new class
            required_config = Namespace()  # 1st requirement for configman
            subordinate_namespace_names = []  # to help the programmer know
                                              # what Namespaces we added
            class_option_name = name_of_class_option  # save the class's option
                                                      # name for the future
            # for each class in the class list
            for a_class in class_list:
                # the un-qualified classname is the namespace name
                namespace_name = a_class.split('.')[-1]
                subordinate_namespace_names.append(namespace_name)
                # create the new Namespace
                required_config[namespace_name] = Namespace()
                # add the option for the class itself
                required_config[namespace_name].add_option(
                    name_of_class_option,
                    #doc=a_class.__doc__  # not helpful if too verbose
                    default=a_class,
                    from_string_converter=class_converter
                )

            @classmethod
            def to_str(cls):
                """this method takes this inner class object and turns it back
                into the original string of classnames.  This is used
                primarily as for the output of the 'help' option"""
                return ', '.join(
                    py_obj_to_str(v[name_of_class_option].value)
                    for v in cls.get_required_config().values()
                    if isinstance(v, Namespace)
                )

        return InnerClassList  # result of class_list_converter
    return class_list_converter  # result of classes_in_namespaces_converter


#==============================================================================
class ServiceBase(DataserviceWebServiceBase):
    """This class is a base class for the wrappers of dataservices.  It
    provides all the common configman parameters and methods that make up
    a dataservice"""

    required_config = Namespace()
    required_config.add_option(
        'impl',
        doc='the qualified name of a class that implements the service',
        default=None,
        from_string_converter=class_converter
    )
    required_config.add_option(
        'output_is_json',
        doc='Does this service provide json output?',
        default=True,
    )
    required_config.add_option(
        'cache_seconds',
        doc='number of seconds to store results in filesystem cache',
        default=3600,
    )
    required_config.add_option(
        'api_whitelist',
        doc='whitelist',
        default={
            'hits': (
                'id',
            )
        },
    )
    required_config.add_option(
        'required_params',
        default=('',),
    )
    required_config.add_option(
        'possible_params',
        default=('',),
    )
    required_config.add_option(
        'method',
        default='post',
        doc='what is the default method for doing what needs to be done',
    )
    required_config.add_option(
        'uri',
        default='',
        doc='Regular expression for matching URLs in Django urls.py'
    )
    required_config.add_option(
        'api_binary_response',
        doc='api binary response',
        default={}
    )
    required_config.add_option(
        'api_binary_filename',
        doc='api binary filename',
        default=None
    )
    required_config.add_option(
        'api_binary_permissions',
        doc='api binary permissions',
        default=()
    )
    required_config.add_option(
        'api_required_permissions',
        doc='api required permissions',
        default=None
    )

    #--------------------------------------------------------------------------
    def __init__(self, config):
        """
        Store the config and create a connection to the database.

        Keyword arguments:
        config -- Configuration of the application.

        """
        super(ServiceBase, self).__init__(config)
        self.crash_store = self.config.crashstorage_class(self.config)

    #--------------------------------------------------------------------------
    @staticmethod
    def parse_versions(versions_list, products):
        """
        Parses the versions, separating by ":" and returning versions
        and products.
        """
        versions = []

        for v in versions_list:
            if v.find(":") > -1:
                pv = v.split(":")
                versions.append(pv[0])
                versions.append(pv[1])
            else:
                products.append(v)

        return (versions, products)

    #--------------------------------------------------------------------------
    @staticmethod
    def prepare_terms(terms, search_mode):
        """
        Prepare terms for search, adding '%' where needed,
        given the search mode.
        """
        if search_mode in ("contains", "starts_with"):
            terms = terms.replace("_", "\_").replace("%", "\%")

        if search_mode == "contains":
            terms = "%" + terms + "%"
        elif search_mode == "starts_with":
            terms = terms + "%"
        return terms

    #--------------------------------------------------------------------------
    @staticmethod
    def dispatch_params(sql_params, key, value):
        """
        Dispatch a parameter or a list of parameters into the params array.
        """
        if not isinstance(value, list):
            sql_params[key] = value
        else:
            for i, elem in enumerate(value):
                sql_params[key + str(i)] = elem
        return sql_params

    #--------------------------------------------------------------------------
    def get_annotated_params(self):
        """return an iterator. One dict for each parameter that the
        class takes.
        Each dict must have the following keys:
            * name
            * type
            * required
        """
        for required, items in (
            (True, getattr(self, 'required_params', [])),
            (False, getattr(self, 'possible_params', []))
        ):
            for item in items:
                if isinstance(item, basestring):
                    type_ = basestring
                    name = item
                elif isinstance(item, dict):
                    type_ = item['type']
                    name = item['name']
                else:
                    assert isinstance(item, tuple)
                    name = item[0]
                    type_ = item[1]

                yield {
                    'name': name,
                    'required': required,
                    'type': type_,
                }

