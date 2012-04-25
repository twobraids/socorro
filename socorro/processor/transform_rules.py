#==============================================================================
# TransformRules predicate and action functions
#    * these function are used for the rewriting of the raw crash file
#          before it is saved in the processed_crash
#    * these functions are used in the processor.raw_rewrite category
#------------------------------------------------------------------------------
def equal_predicate(mapping, external_resource, key, value):
    """a TransformRule predicate function that tests if a key in the mapping
    is equal to a certain value.  In a rule definition, use of this function
    could look like this:

    r = TransformRule('socorro.processor.transform_rules.equal_predicate',
                      '',
                      'key="ReleaseChannel", value="esr",
                      ...)

    parameters:
        mapping - the source mapping from which to test
        external_resource - not used, present for api consistency
        key - the key into the json_doc mapping to test.
        value - the value to compare
    """
    try:
        return mapping[key] == value
    except KeyError:
        return False

#------------------------------------------------------------------------------
def reformat_action(mapping, external_resource, key, format_str):
    """a TransformRule action function that allows a single key in the target
    json file to be rewritten using a format string.  The json itself is used
    as a dict to feed to the format string.  This allows a key's value to be
    rewritten in term of the content of the rest of the json.  The first
    example of this is rewriting the Version string to have a suffix of 'esr'
    if the 'ReleaseChannel' value is 'esr'.  The rule to accomplish this looks
    like this:

    r = TransformRule('socorro.processor.transform_rules.equal_predicate',
                      '',
                      'key="ReleaseChannel", value="esr",  # check for 'esr'
                      'socorro.processor.transform_rules.reformat_action',
                      '',
                      'key="Version", format_str="%(Version)sesr"')

    In this example, the predicate 'transform_rules.equal_predicate' will test
    to see if 'esr' is the value of 'ReleaseChannel'. If true, then the action
    will trigger, using the format string to assign a new value to 'Version'.

    parameters:
        mapping - the source and destination of changes
        external_resource - not used, present for api parellelism with other
                            functions
        key - the key to the entry in the json_doc to change.
        format_str - a standard python format string that will serve as the
                     template for the replacement entry
    """
    mapping[key] = format_str % mapping

#------------------------------------------------------------------------------
def raw_crash_ProductID_predicate(raw_crash, processor):
    """a TransformRule predicate that tests if the value of the json field,
    'ProductID' is present in the processor's productIdMap.  If it is, then
    the action part of the rule will be triggered.

    parameters:
       raw_crash - the source mapping that will be tested
       processor - not used in this context, present only for api consistency
    """
    try:
        return raw_crash['ProductID'] in processor.productIdMap
    except KeyError:
        return False

#------------------------------------------------------------------------------
def raw_crash_Product_rewrite_action(raw_crash, processor):
    """a TransformRule action function that will change the name of a product.
    It finds the new name in by looking up the 'ProductID' in the processor's
    'productIdMap'.

    parameters:
        raw_crash - the destination mapping for the rewrite
        processor - a source for a logger"""
    try:
        product_id = raw_crash['ProductID']
    except KeyError:
        processor.config.logger.debug('ProductID not in json_doc')
        return False
    old_product_name = raw_crash['ProductName']
    new_product_name = processor.productIdMap[product_id]['product_name']
    raw_crash['ProductName'] = new_product_name
    processor.config.logger.info(
      'product name changed from %s to %s based on productID %s',
      old_product_name,
      new_product_name,
      product_id
    )