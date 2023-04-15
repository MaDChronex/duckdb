#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static void ListReverseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto count = args.size();

	Vector nul(0);

	Vector &arrlist = args.data[0];
		if (arrlist.GetType().id() == LogicalTypeId::SQLNULL) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			// result.Reference(nul); //should be NULL not 0
		return;
	}

	UnifiedVectorFormat arrlist_data;
	arrlist.ToUnifiedFormat(count, arrlist_data);
	auto arrlist_entries = (list_entry_t *)arrlist_data.data;

	auto arrlist_list_size = ListVector::GetListSize(arrlist);
	auto &arrlist_child = ListVector::GetEntry(arrlist);
	UnifiedVectorFormat arrlist_child_data;
	arrlist_child.ToUnifiedFormat(arrlist_list_size, arrlist_child_data);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	// Create a reverse selection vector
	SelectionVector rev(arrlist_list_size);
	for (idx_t i = 0; i < arrlist_list_size; i++) {
		rev.set_index(arrlist_list_size - i - 1, i);
	}

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto arrlist_list_index = arrlist_data.sel->get_index(i);

		if (!arrlist_data.validity.RowIsValid(arrlist_list_index)) {
			result_validity.SetInvalid(i);
			continue;
		}
		result_entries[i].offset = offset;
		result_entries[i].length = 0;
		if (arrlist_data.validity.RowIsValid(arrlist_list_index)) {
			const auto &arrlist_entry = arrlist_entries[arrlist_list_index];
			result_entries[i].length += arrlist_entry.length;

			// Create a reverse selection vector
			// SelectionVector rev(arrlist_entry.length);
			// for (idx_t i = 0; i < arrlist_entry.length; i++) {
			// 	rev.set_index(arrlist_entry.length - i - 1, i);
			// }
			ListVector::Append(result, arrlist_child, rev, arrlist_entry.offset + arrlist_entry.length,
			                   arrlist_entry.offset);
		}


		offset += result_entries[i].length;
	}
	D_ASSERT(ListVector::GetListSize(result) == offset);

	if (arrlist.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}


static unique_ptr<FunctionData> ListReverseBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);



		// case LogicalTypeId::SQLNULL:
		// result.SetVectorType(VectorType::CONSTANT_VECTOR);
		// ConstantVector::SetNull(result, true);
		// break;



	auto &arrlist = arguments[0]->return_type;
	if (arrlist.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	} 
	else if (arrlist.id() == LogicalTypeId::SQLNULL) {
		// arrlist = LogicalType::LIST(LogicalType::SQLNULL);
		// throw ParameterNotResolvedException();

		auto return_type = arrlist.id() == LogicalTypeId::SQLNULL ? arrlist : arrlist;
		bound_function.arguments[0] = return_type;
		//bound_function.arguments[1] = return_type;
		bound_function.return_type = return_type;
	} 
	else {
		D_ASSERT(arrlist.id() == LogicalTypeId::LIST);
	
		LogicalType child_type = LogicalType::SQLNULL;
		for (const auto &argument : arguments) {
			child_type = LogicalType::MaxLogicalType(child_type, ListType::GetChildType(argument->return_type));
		}
		auto list_type = LogicalType::LIST(child_type);

		bound_function.arguments[0] = list_type;
		bound_function.return_type = list_type;
	}
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListReverseStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	D_ASSERT(child_stats.size() == 2);

	auto &left_stats = child_stats[0];


	auto stats = left_stats.ToUnique();

	return stats;
}

ScalarFunction ListReverseFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	auto fun = ScalarFunction({LogicalType::LIST(LogicalType::ANY)},
	                          LogicalType::LIST(LogicalType::ANY), ListReverseFunction, ListReverseBind, nullptr,
	                          ListReverseStats);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

void ListReverseFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_reverse", "array_reverse"}, GetFunction());
}

} // namespace duckdb
