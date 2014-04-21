from socorro.lib.transform_rules import is_not_null_predicate

#==============================================================================
# TransformRules predicate and action function section
#    * these function are used for the rewriting of the json file before it is
#          put into Postgres.
#    * these functions are used in the processor.json_rewrite category
#------------------------------------------------------------------------------
def json_equal_predicate(raw_crash, processor, key, value):
    """a TransformRule predicate function that tests if a key in the json
    is equal to a certain value.  In a rule definition, use of this function
    could look like this:

    r = TransformRule('socorro.processor.processor.json_equal_predicate',
                      '',
                      'key="ReleaseChannel", value="esr",
                      ...)

    parameters:
        json_doc - the source mapping from which to test
        processor - not used in this context, present for api consistency
        key - the key into the json_doc mapping to test.
        value - the value to compare
    """
    try:
        return raw_crash[key] == value
    except KeyError:
        return False


#------------------------------------------------------------------------------
def json_reformat_action(raw_crash, processor, key, format_str):
    """a TransformRule action function that allows a single key in the target
    json file to be rewritten using a format string.  The json itself is used
    as a dict to feed to the format string.  This allows a key's value to be
    rewritten in term of the content of the rest of the json.  The first
    example of this is rewriting the Version string to have a suffix of 'esr'
    if the 'ReleaseChannel' value is 'esr'.  The rule to accomplish this looks
    like this:

    r = TransformRule('socorro.processor.processor.json_equal_predicate',
                      '',
                      'key="ReleaseChannel", value="esr",  # check for 'esr'
                      'socorro.processor.processor.json_reformat_action',
                      '',
                      'key="Version", format_str="%(Version)sesr"')

    In this example, the predicate 'processor.json_equal_predicate' will test
    to see if 'esr' is the value of 'ReleaseChannel'. If true, then the action
    will trigger, using the format string to assign a new value to 'Version'.

    parameters:
        json_doc - the source and destination of changes
        processor - not used, present for parellelism with other functions
        key - the key to the entry in the json_doc to change.
        format_str - a standard python format string that will serve as the
                     template for the replacement entry
    """
    raw_crash[key] = format_str % raw_crash


#------------------------------------------------------------------------------
def json_ProductID_predicate(raw_crash, processor):
    """a TransformRule predicate that tests if the value of the json field,
    'ProductID' is present in the processor's _product_id_map.  If it is, then
    the action part of the rule will be triggered.

    parameters:
       json_doc - the source mapping that will be tested
       processor - not used in this context, present only for api consistency
    """
    try:
        return raw_crash['ProductID'] in processor._product_id_map
    except KeyError:
        return False


#------------------------------------------------------------------------------
def json_Product_rewrite_action(raw_crash, processor):
    """a TransformRule action function that will change the name of a product.
    It finds the new name in by looking up the 'ProductID' in the processor's
    '_product_id_map'.

    parameters:
        json_doc - the destination mapping for the rewrite
        processor - a source for a logger"""
    try:
        product_id = raw_crash['ProductID']
    except KeyError:
        processor.config.logger.debug('ProductID not in json_doc')
        return False
    old_product_name = raw_crash['ProductName']
    new_product_name = processor._product_id_map[product_id]['product_name']
    raw_crash['ProductName'] = new_product_name


#------------------------------------------------------------------------------
# the following tuple of tuples is a structure for loading rules into the
# TransformRules system. The tuples take the form:
#   predicate_function, predicate_args, predicate_kwargs,
#   action_function, action_args, action_kwargs.
#
# The args and kwargs components are additional information that a predicate
# or an action might need to have to do its job.  Providing values for args
# or kwargs essentially acts in a manner similar to functools.partial.
# When the predicate or action functions are invoked, these args and kwags
# values will be passed into the function along with the raw_crash,
# processed_crash and processor objects.

default_support_classifier_rules = (
    ( # set the version field to a variant of "version_esr"
        json_equal_predicate,
        (),
        {
            'key': "ReleaseChannel",
            'value': "esr"
        },
        json_reformat_action,
        (),
        {
            'key': 'Version',
            'format_str': "%(Version)sesr"
        }
    ),
    ( # rewrite product name based on values in the processor's _product_id_map
        json_ProductID_predicate,
        (),
        {},
        json_Product_rewrite_action,
        (),
        {}
    ),
    ( # if ProductName is "Webapp Runtime Mobile"
      # change it to "WebappRuntimeMobile"
        json_equal_predicate,
        (),
        {
            'key': "ProductName",
            'value': "Webapp Runtime Mobile"
        },
        json_reformat_action,
        (),
        {
            'key': 'ProductName',
            'format_str': 'WebappRuntimeMobile'
        }
    ),
    ( # if ProductName is "Webapp Runtime"
      # change it to "WebappRuntim."
        json_equal_predicate,
        (),
        {
            'key': "ProductName",
            'value': "Webapp Runtime"
        },
        json_reformat_action,
        (),
        {
            'key': 'ProductName',
            'format_str': 'WebappRuntime'
        }
    ),
    ( # set the URL field to that of the PluginContentURL
        is_not_null_predicate,
        (),
        {'key': "PluginContentURL"},
        json_reformat_action,
        (),
        {
            'key': 'URL',
            'format_str': '%(PluginContentURL)s'
        }
    ),
    ( # set the 'Comments' field to the same as 'PluginUserComment'
        is_not_null_predicate,
        (),
        {'key': 'PluginUserComment'},
        json_reformat_action,
        (),
        {
            'key': 'Comments',
            'format_str': "%(PluginUserComment)s"
        }
    ),
)
