-ifndef(VMQ_REG_TRIE_HRL).
-define(VMQ_REG_TRIE_HRL, true).

-record(trie, {edge, node_id}).
-record(trie_node, {node_id, edge_count = 0, topic}).
-record(trie_edge, {node_id, word}).

-endif.
