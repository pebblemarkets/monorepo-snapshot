#![allow(clippy::doc_overindented_list_items)]
#![allow(clippy::large_enum_variant)]

pub mod com {
    pub mod daml {
        pub mod ledger {
            pub mod api {
                pub mod v2 {
                    tonic::include_proto!("com.daml.ledger.api.v2");
                }
            }
        }
    }
}

pub mod google {
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
}
