class GraphQL::DefinitionHelpers::DefinedByConfig::DefinitionConfig
  # Wraps a field definition with a ConnectionField
  # - applies default fields
  # - wraps the resolve proc to make a connection
  #
  def connection(name, type = nil, desc = nil, property: nil, &block)
    # Wrap the given block to define the default args
    definition_block = -> (config) {
      argument :first, types.Int
      argument :after, types.String
      argument :last, types.Int
      argument :before, types.String
      argument :order, types.String
      self.instance_eval(&block)
    }
    connection_field = field(name, type, desc, property: property, &definition_block)
    # Wrap the defined resolve proc
    # TODO: make a public API on GraphQL::Field to expose this proc
    original_resolve = connection_field.instance_variable_get(:@resolve_proc)
    connection_resolve = -> (obj, args, ctx) {
      items = original_resolve.call(obj, args, ctx)
      connection_class = GraphQL::Relay::BaseConnection.connection_for_items(items)
      connection_class.new(items, args)
    }
    connection_field.resolve = connection_resolve
    fields[name.to_s] = connection_field
  end

  alias :return_field :field
  alias :return_fields :fields

  def global_id_field(field_name)
    name || raise("You must define the type's name before creating a GlobalIdField")
    field(name, field: GraphQL::Relay::GlobalIdField.new(name))
  end
end