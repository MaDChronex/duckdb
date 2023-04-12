#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static void ListReverseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto count = args.size();

	Vector &lhs = args.data[0];
	// Vector &rhs = args.data[1];
	// if (lhs.GetType().id() == LogicalTypeId::SQLNULL) {
	// 	result.Reference(rhs);
	// 	return;
	// }
	// if (rhs.GetType().id() == LogicalTypeId::SQLNULL) {
	// 	result.Reference(lhs);
	// 	return;
	// }

	UnifiedVectorFormat lhs_data;
	//UnifiedVectorFormat rhs_data;
	lhs.ToUnifiedFormat(count, lhs_data);
	// rhs.ToUnifiedFormat(count, rhs_data);
	auto lhs_entries = (list_entry_t *)lhs_data.data;
	// auto rhs_entries = (list_entry_t *)rhs_data.data;

	auto lhs_list_size = ListVector::GetListSize(lhs);
	// auto rhs_list_size = ListVector::GetListSize(rhs);
	auto &lhs_child = ListVector::GetEntry(lhs);
	// auto &rhs_child = ListVector::GetEntry(rhs);
	UnifiedVectorFormat lhs_child_data;
	// UnifiedVectorFormat rhs_child_data;
	lhs_child.ToUnifiedFormat(lhs_list_size, lhs_child_data);
	// rhs_child.ToUnifiedFormat(rhs_list_size, rhs_child_data);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto lhs_list_index = lhs_data.sel->get_index(i);
		
		//lhs_data.sel.set_index(count - i - 1, i);

		// auto rhs_list_index = rhs_data.sel->get_index(i);
		if (!lhs_data.validity.RowIsValid(lhs_list_index)
		// && !rhs_data.validity.RowIsValid(rhs_list_index)
		) {
			result_validity.SetInvalid(i);
			continue;
		}
		result_entries[i].offset = offset;
		result_entries[i].length = 0;
		if (lhs_data.validity.RowIsValid(lhs_list_index)) {
			const auto &lhs_entry = lhs_entries[lhs_list_index];
			result_entries[i].length += lhs_entry.length;
			// This is goood------------------
			// Vector rev = Vector(lhs_child, *lhs_child_data.sel, lhs_entry.length);
			// for (idx_t i = 0; i < lhs_entry.length; i++) {
			// 	rev.SetValue(i, result.GetValue(lhs_entry.length - i - 1));
			// 	rev.Print(i);
			// 	//lhs_child_data.sel->set_index(lhs_entry.length - i - 1, i);
			// 	//lhs_child_data.sel->Print(lhs_entry.length-i-1);
			// }
			//--------------------------------
			SelectionVector rev(lhs_entry.length);
			for (idx_t i = 0; i < lhs_entry.length; i++) {
				rev.set_index(lhs_entry.length - i - 1, i);
				//rev.Print(lhs_entry.length-i-1);
				//lhs_child_data.sel->set_index(lhs_entry.length - i - 1, i);
				//lhs_child_data.sel->Print(lhs_entry.length-i-1);
			}
			ListVector::Append(result, lhs_child, rev, lhs_entry.offset + lhs_entry.length,
			                   lhs_entry.offset);
			// DUCKDB_API static void Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
	        //                       idx_t source_offset = 0);
		}

		// if (rhs_data.validity.RowIsValid(rhs_list_index)) {
		// 	const auto &rhs_entry = rhs_entries[rhs_list_index];
		// 	result_entries[i].length += rhs_entry.length;
		// 	ListVector::Append(result, rhs_child, *rhs_child_data.sel, rhs_entry.offset + rhs_entry.length,
		// 	                   rhs_entry.offset);
		// }
		offset += result_entries[i].length;
	}
	D_ASSERT(ListVector::GetListSize(result) == offset);

	if (lhs.GetVectorType() == VectorType::CONSTANT_VECTOR
	//  && rhs.GetVectorType() == VectorType::CONSTANT_VECTOR
	 ) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	// SelectionVector selection_V = SelectionVector(count);
	// for (idx_t i = 0; i < count; i++) {
	// 	selection_V.set_index(count - i - 1, i);
	// 	selection_V.Print(count);
	// }
	
	// Vector result_rev = Vector (result, selection_V, count);
	
	// result.Reinterpret(result_rev);
}



static unique_ptr<FunctionData> ListReverseBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);

	auto &lhs = arguments[0]->return_type;
	//auto &rhs = arguments[1]->return_type;
	if (lhs.id() == LogicalTypeId::UNKNOWN 
	// || rhs.id() == LogicalTypeId::UNKNOWN
	) {
		throw ParameterNotResolvedException();
	} else if (lhs.id() == LogicalTypeId::SQLNULL 
	// || rhs.id() == LogicalTypeId::SQLNULL
	) {
		// we mimic postgres behaviour: list_id(NULL, my_list) = my_list
		bound_function.arguments[0] = lhs;
	} else {
		D_ASSERT(lhs.id() == LogicalTypeId::LIST);
	
	// 	auto return_type = rhs.id() == LogicalTypeId::SQLNULL ? lhs: rhs;
	// 	bound_function.arguments[0] = return_type;
	// 	bound_function.arguments[1] = return_type;
	// 	bound_function.return_type = return_type;
	// } else {
	// 	D_ASSERT(lhs.id() == LogicalTypeId::LIST);
	// 	D_ASSERT(rhs.id() == LogicalTypeId::LIST);

		// Resolve list type
		LogicalType child_type = LogicalType::SQLNULL;
		for (const auto &argument : arguments) {
			child_type = LogicalType::MaxLogicalType(child_type, ListType::GetChildType(argument->return_type));
		}
		auto list_type = LogicalType::LIST(child_type);

		bound_function.arguments[0] = list_type;
		// bound_function.arguments[1] = list_type;
		bound_function.return_type = list_type;
	}
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListReverseStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	D_ASSERT(child_stats.size() == 2);

	auto &left_stats = child_stats[0];
	// auto &right_stats = child_stats[1];


	auto stats = left_stats.ToUnique();
	// stats->Merge(right_stats);

	return stats;
}

ScalarFunction ListReverseFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	// this is where the arguments are defined
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
