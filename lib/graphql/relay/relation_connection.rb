module GraphQL
  module Relay
    class RelationConnection < BaseConnection
      DEFAULT_ORDER = "id"

      def order_values
        @order_values ||= object.order_values
      end

      def order_names
        @order_names ||= begin
          if order_values.count == 0
            names = [DEFAULT_ORDER]
          else
            names = order_values.map { |order_value|
              order_value.expr.name
            }
          end
          names
        end
      end

      def cursor_from_node(item)
        item_values = order_names.map { |order_name|
          item.public_send(order_name)
        }
        cursor_parts = [order_names, item_values].map { |part|
          Marshal.dump(part)
        }
        Base64.strict_encode64(cursor_parts.join(CURSOR_SEPARATOR))
      end

      def has_next_page
        !!(first && sliced_nodes.limit(first + 1).count > first)
      end

      # Used by `pageInfo`
      def has_previous_page
        !!(last && sliced_nodes.limit(last + 1).count > last)
      end

      # apply first / last limit results
      def paged_nodes
        @paged_nodes = begin
          items = sliced_nodes
          limit = [first, last, max_page_size].compact.min
          first && items = items.first(limit)
          last && items.count > last && items = items.last(limit)
          items
        end
      end

      def sliced_nodes
        @sliced_nodes ||= begin
          items = object

          if after
            item_values = hash_from_cursor(after)
            where_conditions = create_order_conditions(item_values, :after)
            items = items.where(where_conditions)
          end

          if before
            item_values = hash_from_cursor(before)
            where_conditions = create_order_conditions(item_values, :before)
            items = items.where(where_conditions)
          end

          items
        end
      end

      def hash_from_cursor(cursor)
        orders, item_values = slice_from_cursor(cursor)
        Hash[orders.zip(item_values)]
      end

      def slice_from_cursor(cursor)
        decoded = Base64.decode64(cursor)
        orders, item_values = decoded.split(CURSOR_SEPARATOR).map { |dump|
          Marshal.load(dump)
        }
      end

      def table_name
        @table_name ||= object.table.table_name
      end

      def create_direction_marker(order_name, argument)
        if order_values.count > 0
          direction = order_values.select { |order_value|
            order_value.expr.name == order_name
          }.first.direction
        else
          direction = :asc
        end

        if argument == :after
          marker = direction == :asc ? ">" : "<"
        elsif argument == :before
          marker = direction == :asc ? "<" : ">"
        end
      end

      def create_order_conditions(item_values, argument)
        where_conditions = item_values.map { |order_name, item_value|
          direction_marker = create_direction_marker(order_name, argument)
          condition = create_order_condition(table_name, order_name, item_value, direction_marker)
          condition
        }
        query = where_conditions.map { |condition|
          condition[0]
        }.join(' AND ')
        arguments = where_conditions.map { |condition|
          condition[1]
        }
        [query, arguments].flatten
      end

      # When creating the where constraint, cast the value to correct column data type so
      # active record can send it in correct format to db
      def create_order_condition(table, column, value, direction_marker)
        table_name = ActiveRecord::Base.connection.quote_table_name(table)
        name = ActiveRecord::Base.connection.quote_column_name(column)
        if ActiveRecord::VERSION::MAJOR == 5
          casted_value = object.table.able_to_type_cast? ? object.table.type_cast_for_database(column, value) : value
        elsif ActiveRecord::VERSION::MAJOR == 4 && ActiveRecord::VERSION::MINOR >= 2
          casted_value = object.table.engine.columns_hash[column].cast_type.type_cast_from_user(value)
        else
          casted_value = object.table.engine.columns_hash[column].type_cast(value)
        end
        ["#{table_name}.#{name} #{direction_marker} ?", casted_value]
      end

      if defined?(ActiveRecord)
        BaseConnection.register_connection_implementation(ActiveRecord::Relation, RelationConnection)
      end
    end
  end
end
