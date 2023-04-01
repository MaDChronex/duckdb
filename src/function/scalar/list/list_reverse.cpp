#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/serializer/enum_serializer.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/main/config.hpp"

#include "duckdb/common/sort/sort.hpp"

namespace duckdb {

struct ListReverseBindData : public FunctionData {
	ListReverseBindData(OrderType order_type_p, OrderByNullType null_order_p, const LogicalType &return_type_p,
	                 const LogicalType &child_type_p, ClientContext &context_p);
	~ListReverseBindData() override;

	OrderType order_type;
	OrderByNullType null_order;
	LogicalType return_type;
	LogicalType child_type;

	vector<LogicalType> types;
	vector<LogicalType> payload_types;

	ClientContext &context;
	RowLayout payload_layout;
	vector<BoundOrderByNode> orders;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
};

ListReverseBindData::ListReverseBindData(OrderType order_type_p, OrderByNullType null_order_p,
                                   const LogicalType &return_type_p, const LogicalType &child_type_p,
                                   ClientContext &context_p)
    : order_type(order_type_p), null_order(null_order_p), return_type(return_type_p), child_type(child_type_p),
      context(context_p) {

	// get the vector types
	types.emplace_back(LogicalType::USMALLINT);
	types.emplace_back(child_type);
	D_ASSERT(types.size() == 2);

	// get the payload types
	payload_types.emplace_back(LogicalType::UINTEGER);
	D_ASSERT(payload_types.size() == 1);

	// initialize the payload layout
	payload_layout.Initialize(payload_types);

	// get the BoundOrderByNode
	auto idx_col_expr = make_unique_base<Expression, BoundReferenceExpression>(LogicalType::USMALLINT, 0);
	auto lists_col_expr = make_unique_base<Expression, BoundReferenceExpression>(child_type, 1);
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, std::move(idx_col_expr));
	orders.emplace_back(order_type, null_order, std::move(lists_col_expr));
}

unique_ptr<FunctionData> ListReverseBindData::Copy() const {
	return make_unique<ListReverseBindData>(order_type, null_order, return_type, child_type, context);
}

bool ListReverseBindData::Equals(const FunctionData &other_p) const {
	auto &other = (ListReverseBindData &)other_p;
	return order_type == other.order_type && null_order == other.null_order;
}

ListReverseBindData::~ListReverseBindData() {
}

// create the key_chunk and the payload_chunk and sink them into the local_sort_state
void SinkDataChunkRev(Vector *child_vector, SelectionVector &sel, idx_t offset_lists_indices, vector<LogicalType> &types,
                   vector<LogicalType> &payload_types, Vector &payload_vector, LocalSortState &local_sort_state,
                   bool &data_to_sort, Vector &lists_indices) {

	// slice the child vector
	Vector slice(*child_vector, sel, offset_lists_indices);

	// initialize and fill key_chunk
	DataChunk key_chunk;
	key_chunk.InitializeEmpty(types);
	key_chunk.data[0].Reference(lists_indices);
	key_chunk.data[1].Reference(slice);
	key_chunk.SetCardinality(offset_lists_indices);

	// initialize and fill key_chunk and payload_chunk
	DataChunk payload_chunk;
	payload_chunk.InitializeEmpty(payload_types);
	payload_chunk.data[0].Reference(payload_vector);
	payload_chunk.SetCardinality(offset_lists_indices);

	key_chunk.Verify();
	payload_chunk.Verify();

	// sink
	key_chunk.Flatten();
	local_sort_state.SinkChunk(key_chunk, payload_chunk);
	data_to_sort = true;
}

static void ListReverseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() >= 1 && args.ColumnCount() <= 3);
	auto count = args.size();
	Vector &input_lists = args.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);

	if (input_lists.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (ListReverseBindData &)*func_expr.bind_info;

	// initialize the global and local sorting state
	auto &buffer_manager = BufferManager::GetBufferManager(info.context);
	GlobalSortState global_sort_state(buffer_manager, info.orders, info.payload_layout);
	LocalSortState local_sort_state;
	local_sort_state.Initialize(global_sort_state, buffer_manager);

	// this ensures that we do not change the order of the entries in the input chunk
	VectorOperations::Copy(input_lists, result, count, 0, 0);

	// get the child vector
	auto lists_size = ListVector::GetListSize(result);
	auto &child_vector = ListVector::GetEntry(result);
	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(lists_size, child_data);

	// get the lists data
	UnifiedVectorFormat lists_data;
	result.ToUnifiedFormat(count, lists_data);
	auto list_entries = (list_entry_t *)lists_data.data;

	// create the lists_indices vector, this contains an element for each list's entry,
	// the element corresponds to the list's index, e.g. for [1, 2, 4], [5, 4]
	// lists_indices contains [0, 0, 0, 1, 1]
	Vector lists_indices(LogicalType::USMALLINT);
	auto lists_indices_data = FlatVector::GetData<uint16_t>(lists_indices);

	// create the payload_vector, this is just a vector containing incrementing integers
	// this will later be used as the 'new' selection vector of the child_vector, after
	// rearranging the payload according to the sorting order
	Vector payload_vector(LogicalType::UINTEGER);
	auto payload_vector_data = FlatVector::GetData<uint32_t>(payload_vector);

	// selection vector pointing to the data of the child vector,
	// used for slicing the child_vector correctly
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	idx_t offset_lists_indices = 0;
	uint32_t incr_payload_count = 0;
	bool data_to_sort = false;

	for (idx_t i = 0; i < count; i++) {
		auto lists_index = lists_data.sel->get_index(i);
		const auto &list_entry = list_entries[lists_index];

		// nothing to do for this list
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		// empty list, no sorting required
		if (list_entry.length == 0) {
			continue;
		}

		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			// lists_indices vector is full, sink
			if (offset_lists_indices == STANDARD_VECTOR_SIZE) {
				SinkDataChunkRev(&child_vector, sel, offset_lists_indices, info.types, info.payload_types, payload_vector,
				              local_sort_state, data_to_sort, lists_indices);
				offset_lists_indices = 0;
			}

			auto source_idx = list_entry.offset + child_idx;
            // this is where the SelectionVector index is set.
			//sel.set_index(offset_lists_indices, list_entry.length - source_idx - 0); // This was a change!!!
			// previous code:
			//sel.set_index(offset_lists_indices, source_idx);
			lists_indices_data[offset_lists_indices] = (uint32_t)i;
			payload_vector_data[offset_lists_indices] = source_idx;
			offset_lists_indices++;
			incr_payload_count++;

		// Reverse the order of sel.
		}
		for (i = 0; i < list_entry.length; i++)  {
			sel.set_index(i, list_entry.length - i - 1);
		}
	}

	if (offset_lists_indices != 0) {
		SinkDataChunkRev(&child_vector, sel, offset_lists_indices, info.types, info.payload_types, payload_vector,
		              local_sort_state, data_to_sort, lists_indices);
	}

	if (data_to_sort) {
		// add local state to global state, which sorts the data
		global_sort_state.AddLocalState(local_sort_state);
		global_sort_state.PrepareMergePhase();

		// selection vector that is to be filled with the 'sorted' payload
		SelectionVector sel_sorted(incr_payload_count);
		idx_t sel_sorted_idx = 0;

		// scan the sorted row data
		PayloadScanner scanner(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state);
		for (;;) {
			DataChunk result_chunk;
			result_chunk.Initialize(Allocator::DefaultAllocator(), info.payload_types);
			result_chunk.SetCardinality(0);
			scanner.Scan(result_chunk);
			if (result_chunk.size() == 0) {
				break;
			}

			// construct the selection vector with the new order from the result vectors
			Vector result_vector(result_chunk.data[0]);
			auto result_data = FlatVector::GetData<uint32_t>(result_vector);
			auto row_count = result_chunk.size();

			for (idx_t i = 0; i < row_count; i++) {
				sel_sorted.set_index(sel_sorted_idx, result_data[i]);
				D_ASSERT(result_data[i] < lists_size);
				sel_sorted_idx++;
			}
		}

		D_ASSERT(sel_sorted_idx == incr_payload_count);
		child_vector.Slice(sel_sorted, sel_sorted_idx);
		child_vector.Flatten(sel_sorted_idx);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListReverseBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments, OrderType &order,
                                             OrderByNullType &null_order) {
	bound_function.arguments[0] = arguments[0]->return_type;
	bound_function.return_type = arguments[0]->return_type;
	auto child_type = ListType::GetChildType(arguments[0]->return_type);

	return make_unique<ListReverseBindData>(order, null_order, bound_function.return_type, child_type, context);
}

// static OrderByNullType GetNullOrder(ClientContext &context, vector<unique_ptr<Expression>> &arguments, idx_t idx) {

// 	if (!arguments[idx]->IsFoldable()) {
// 		throw InvalidInputException("Null sorting order must be a constant");
// 	}
// 	Value null_order_value = ExpressionExecutor::EvaluateScalar(context, *arguments[idx]);
// 	auto null_order_name = StringUtil::Upper(null_order_value.ToString());
// 	const auto null_order_arg = EnumSerializer::StringToEnum<OrderByNullType>(null_order_name.c_str());
// 	switch (null_order_arg) {
// 	case OrderByNullType::NULLS_FIRST:
// 	case OrderByNullType::NULLS_LAST:
// 		return null_order_arg;
// 	case OrderByNullType::ORDER_DEFAULT:
// 		return DBConfig::GetConfig(context).options.default_null_order;
// 	default:
// 		throw InvalidInputException("Null sorting order must be either NULLS FIRST, NULLS LAST or DEFAULT");
// 	}
// }

static unique_ptr<FunctionData> ListNormalReverseBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(!bound_function.arguments.empty() && bound_function.arguments.size() <= 3);
	D_ASSERT(!arguments.empty() && arguments.size() <= 3);

	// set default values
	auto &config = DBConfig::GetConfig(context);
	auto order = config.options.default_order_type;
	auto null_order = config.options.default_null_order;

	// get the sorting order
	if (arguments.size() >= 2) {

		if (!arguments[1]->IsFoldable()) {
			throw InvalidInputException("Sorting order must be a constant");
		}
		Value order_value = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);

		const auto order_name = StringUtil::Upper(order_value.ToString());
		const auto order_arg = EnumSerializer::StringToEnum<OrderType>(order_name.c_str());
		switch (order_arg) {
		case OrderType::ASCENDING:
		case OrderType::DESCENDING:
			order = order_arg;
			break;
		case OrderType::ORDER_DEFAULT:
			break;
		default:
			throw InvalidInputException("Sorting order must be either ASC, DESC or DEFAULT");
		}
	}

	// // get the null sorting order
	// if (arguments.size() == 3) {
	// 	null_order = GetNullOrder(context, arguments, 2);
	// }

	return ListReverseBind(context, bound_function, arguments, order, null_order);
}

// static unique_ptr<FunctionData> ListReverseSortBind(ClientContext &context, ScalarFunction &bound_function,
//                                                     vector<unique_ptr<Expression>> &arguments) {

// 	D_ASSERT(bound_function.arguments.size() == 1 || bound_function.arguments.size() == 2);
// 	D_ASSERT(arguments.size() == 1 || arguments.size() == 2);

// 	// set (reverse) default values
// 	auto &config = DBConfig::GetConfig(context);
// 	auto order =
// 	    (config.options.default_order_type == OrderType::ASCENDING) ? OrderType::DESCENDING : OrderType::ASCENDING;
// 	auto null_order = config.options.default_null_order;

// 	// get the null sorting order
// 	if (arguments.size() == 2) {
// 		null_order = GetNullOrder(context, arguments, 1);
// 	}

// 	return ListReverseBind(context, bound_function, arguments, order, null_order);
// }

void ListReverseFun::RegisterFunction(BuiltinFunctions &set) {

	// normal sort

	// one parameter: list
	ScalarFunction sort({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY), ListReverseFunction,
	                    ListNormalReverseBind);

	// two parameters: list, order
	ScalarFunction sort_order({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR},
	                          LogicalType::LIST(LogicalType::ANY), ListReverseFunction, ListNormalReverseBind);

	// three parameters: list, order, null order
	ScalarFunction sort_orders({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR, LogicalType::VARCHAR},
	                           LogicalType::LIST(LogicalType::ANY), ListReverseFunction, ListNormalReverseBind);

	ScalarFunctionSet list_sort("list_reverse");
	list_sort.AddFunction(sort);
	list_sort.AddFunction(sort_order);
	list_sort.AddFunction(sort_orders);
	set.AddFunction(list_sort);

	ScalarFunctionSet array_sort("array_reverse");
	array_sort.AddFunction(sort);
	array_sort.AddFunction(sort_order);
	array_sort.AddFunction(sort_orders);
	set.AddFunction(array_sort);
