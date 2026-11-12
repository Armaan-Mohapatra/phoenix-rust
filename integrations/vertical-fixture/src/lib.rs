//! Synthetic vertical integration test. No exchange connectivity or real data.
#[cfg(test)]
mod tests {
    use phoenix_domain::{BinaryFinal,Evidence,EvidenceTier,FeeRate,Price,Side};
    use phoenix_l2::L2Session;
    use phoenix_kalshi_wire::{decode,Message};
    use phoenix_parquet_batch::Row;
    use phoenix_durable_archive::{Archive,EventRow,SourceScope};
    use phoenix_pipeline::{evaluate_case,Case,Stage,Error as PipelineError};
    use phoenix_research::{Split,SplitPlan};
    use phoenix_settlement::{compare_window_averages,IndexTick,WindowRule};
    use phoenix_strategies::{FrozenSpec,ModelSignal,Recipe};
    use sha2::{Digest,Sha256};
    use tempfile::TempDir;

    fn p(value:u16)->Price {Price::new(value).unwrap()}
    fn snapshots()->(&'static str,&'static str){
        (r#"{
            "type":"orderbook_snapshot","sending_ts_ms":1000,"sid":4,"seq":1,
            "msg":{"market_ticker":"SYNTHETIC-BTC","clock":100,
            "yes_dollars_fp":[["0.4000","5.00"]],
            "no_dollars_fp":[["0.5500","5.00"]]}}"#,
        r#"{
            "type":"orderbook_delta","sending_ts_ms":1001,"sid":4,"seq":2,
            "msg":{"market_ticker":"SYNTHETIC-BTC","clock":101,
            "side":"yes","price_dollars":"0.4000","delta_fp":"1.00"}}"#)
    }
    fn scope()->SourceScope {SourceScope{
        feed:"kalshi".into(),session:"synthetic-session".into(),
        market:"SYNTHETIC-BTC".into(),observed_sequence_gaps:0,
        quality_flags:vec!["SYNTHETIC_ONLY".into()],source_manifest_sha256:None,
    }}
    fn record(seq:u64,raw:&str,received_ns:i64)->EventRow{
        EventRow{event_id:format!("synthetic-s4:{seq}"),
            data:Row{feed:"kalshi".into(),session:"synthetic-session".into(),
                market:"SYNTHETIC-BTC".into(),sequence:seq,
                kind:if seq==1{1}else{2},exchange_ns:(999+seq as i64)*1_000_000,
                received_ns,payload:raw.as_bytes().to_vec()}}
    }
    fn complete_case(source_hash:String)->Case{
        let (raw1,raw2)=snapshots();
        let mut books=Vec::new();
        let mut l2=L2Session::new(4,"synthetic-session".into());
        match decode(raw1,1_200_000_000).unwrap() {
            Message::Snapshot(s)=>l2.on_snapshot(s).unwrap(),
            _=>panic!("expected snapshot")
        };
        books.push(l2.ready_integer_book("SYNTHETIC-BTC").unwrap());
        match decode(raw2,1_210_000_000).unwrap() {
            Message::Delta(d)=>l2.on_delta(d).unwrap(),
            _=>panic!("expected delta"),
        }
        books.push(l2.ready_integer_book("SYNTHETIC-BTC").unwrap());
        let outcome=BinaryFinal{
            market:"SYNTHETIC-BTC".into(),rules_sha256:"f".repeat(64),
            winner:Side::Yes,venue_final:Evidence{
                source_id:"synthetic-venue".into(),content_sha256:"a".repeat(64)},
            independent:None,
        };
        Case{
            event_id:"synthetic-event".into(),market:"SYNTHETIC-BTC".into(),
            source_sha256:source_hash,source_sealed:true,
            source_sequence_complete:true,
            books,predictions:vec![ModelSignal{asof_received_ns:1_250_000_000,
                probability_yes:p(7_000),model_weight:1}],
            strategy:FrozenSpec{id:"synthetic-favorite-v1".into(),
                code_sha256:"b".repeat(64),config_sha256:"c".repeat(64),
                recipe:Recipe::ModelEdge,min_net_edge_ticks:200,
                max_quote_age_ns:500_000_000,fee_rate:FeeRate::STANDARD_TAKER,
                max_spread_ticks:Some(1000)},
            split_plan:SplitPlan{discovery_end_ns:100,validation_end_ns:2_000_000_000,
                untouched_start_ns:3_000_000_000,embargo_ns:10},
            feature_start_ns:1_200_000_000,outcome_end_ns:1_900_000_000,
            decision_ns:1_300_000_000,modeled_arrival_ns:1_400_000_000,
            quantity:1,cash_centicents:100_000,
            rules_sha256:"f".repeat(64),finalized:Some(outcome),
        }
    }
    #[test]
    fn synthetic_market_data_to_recoverable_archive_to_modeled_pnl(){
        let temp=TempDir::new().unwrap();
        let path=temp.path().join("research");
        let (raw1,raw2)=snapshots();
        let archive_rows=[
            record(1,raw1,1_200_000_000),
            record(2,raw2,1_210_000_000),
        ];
        let mut archive=Archive::open(&path).unwrap();
        let published=archive.publish(scope(),&archive_rows).unwrap();
        assert_eq!(published.added,2);
        assert_eq!(archive.verify_all().unwrap(),1);
        let source_hash=published.partition_id.unwrap();
        drop(archive);
        let mut reopened=Archive::open(&path).unwrap();
        assert_eq!(reopened.publish(scope(),&archive_rows).unwrap().duplicates,2);
        assert_eq!(reopened.indexed_events(),2);
        let c=complete_case(source_hash);
        let first=evaluate_case(&c).unwrap();
        let second=evaluate_case(&c).unwrap();
        assert_eq!(first,second);
        assert_eq!(first.split,Split::Validation);
        assert_eq!(first.stage,Stage::ModeledSettled);
        assert_eq!(first.modeled_quantity,1);
        assert_eq!(first.tier,Some(EvidenceTier::VenueFinal));
        assert_eq!(first.net_pnl_centicents,Some(5_326));
        assert_eq!(first.fees_centicents,174);
    }
    #[test]
    fn incomplete_or_pending_input_never_produces_realized_pnl(){
        let c=complete_case("e".repeat(64));
        let mut pending=c;
        pending.finalized=None;
        let r=evaluate_case(&pending).unwrap();
        assert_eq!(r.stage,Stage::PendingSettlement);
        assert_eq!(r.net_pnl_centicents,None);
        pending.source_sealed=false;
        assert_eq!(evaluate_case(&pending),Err(PipelineError::UnsealedSource));
    }
    #[test]
    fn reference_window_is_derived_but_not_authenticated_settlement(){
        let values=(0..120).map(|i|IndexTick{
            second_utc:i,value_1e8:if i<60{100_000_000}else{100_100_000}
        }).collect::<Vec<_>>();
        let observed=compare_window_averages(&values,WindowRule{
            opening_start_sec:0,closing_start_sec:60,
            samples_per_window:60,tie_resolves_yes:false,
        }).unwrap();
        assert_eq!(observed.derived_winner,Side::Yes);
        // This test has no verified external source; it must not create an
        // independent-evidence marker from a synthetic derived outcome.
        let c=complete_case("e".repeat(64));
        assert_eq!(c.finalized.unwrap().independent,None);
    }
}