//! Bounded exact-arithmetic regression on a minimally projected REAL historical
//! Phoenix paper-ledger export. Source is user-supplied and READ-ONLY.
//! This does NOT authenticate exchange fills, official final or independent
//! reference settlement. No real orders, credentials or source-path access.
use serde::Deserialize;
#[derive(Debug,Clone,Deserialize)]
pub struct LedgerRow {
    pub interval_start_utc:String,
    pub interval_end_utc:String,
    pub record_type:String,
    pub decision_time_utc:String,
    pub ticker:String,
    pub side:String,
    pub quantity:String,
    pub entry_price:String,
    pub fee:String,
    pub total_cost:String,
    pub settlement_status:String,
    pub settled_outcome:String,
    pub realized_pnl:String,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Error {MissingData,InvalidDecimal,UnsupportedQuantity,UnsupportedType,
    InconsistentLedger,UnresolvedOutcome}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct ResultRow {
    pub ticker:String,pub side:String,
    pub net_centicents:i128,pub recorded_net_centicents:i128,
    pub matches_archived_arithmetic:bool,
    pub is_actual_venue_fill:bool,
    pub independent_settlement_verified:bool,
}
/// Decimal string -> 1/10_000 USD without rounding (extra zero places OK).
pub fn money(value:&str)->Result<i128,Error>{
    let negative=value.starts_with('-');
    let s=if negative {&value[1..]}else{value};
    let (whole,frac)=s.split_once('.').unwrap_or((s,""));
    if whole.is_empty()||!whole.bytes().all(|x|x.is_ascii_digit())||
       !frac.bytes().all(|x|x.is_ascii_digit()) {return Err(Error::InvalidDecimal);}
    let significant=frac.trim_end_matches('0');
    if significant.len()>4 {return Err(Error::InvalidDecimal);}
    let v=whole.parse::<i128>().map_err(|_|Error::InvalidDecimal)?
        .checked_mul(10_000).ok_or(Error::InvalidDecimal)?;
    let decimal=if significant.is_empty(){0}else{
        significant.parse::<i128>().map_err(|_|Error::InvalidDecimal)?
            .checked_mul(10_i128.pow((4-significant.len())as u32))
            .ok_or(Error::InvalidDecimal)?
    };
    let result=v.checked_add(decimal).ok_or(Error::InvalidDecimal)?;
    if negative {result.checked_neg().ok_or(Error::InvalidDecimal)} else {Ok(result)}
}
pub fn reconcile_venue_reported_binary(r:&LedgerRow)->Result<ResultRow,Error>{
    if r.record_type!="PAPER"{return Err(Error::UnsupportedType);}
    if r.settlement_status!="SETTLED_YES"&&r.settlement_status!="SETTLED_NO"{
        return Err(Error::UnresolvedOutcome);
    }
    if !matches!(r.side.as_str(),"YES"|"NO")||
        !matches!(r.settled_outcome.as_str(),"YES"|"NO")||
        r.ticker.trim().is_empty()||r.decision_time_utc>=r.interval_end_utc||
        r.interval_start_utc>=r.decision_time_utc {
        return Err(Error::MissingData);
    }
    let q=money(&r.quantity)?;
    if q!=10_000{return Err(Error::UnsupportedQuantity);}
    let cost=money(&r.entry_price)?;
    let fee=money(&r.fee)?;
    let total=cost.checked_add(fee).ok_or(Error::InvalidDecimal)?;
    if total!=money(&r.total_cost)?{return Err(Error::InconsistentLedger);}
    let payout=if r.side==r.settled_outcome{10_000}else{0};
    let calculated=payout-total;
    let recorded=money(&r.realized_pnl)?;
    Ok(ResultRow {ticker:r.ticker.clone(),side:r.side.clone(),
        net_centicents:calculated,recorded_net_centicents:recorded,
        matches_archived_arithmetic:calculated==recorded,
        is_actual_venue_fill:false,
        independent_settlement_verified:false,
    })
}
pub fn parse_bounded_csv(data:&str)->Result<Vec<LedgerRow>,Error>{
    if data.len()>16*1024{return Err(Error::MissingData);}
    let mut reader=csv::ReaderBuilder::new().has_headers(true)
        .from_reader(data.as_bytes());
    let mut rows=Vec::new();
    for row in reader.deserialize::<LedgerRow>(){
        if rows.len()>=16{return Err(Error::MissingData);}
        rows.push(row.map_err(|_|Error::MissingData)?);
    }
    if rows.is_empty(){return Err(Error::MissingData);}
    Ok(rows)
}
#[cfg(test)]
mod tests{
    use super::*;
    /// These two rows were extracted as a minimal non-secret projection
    /// from an uploaded historical Phoenix archive, not fabricated prices.
    const INPUT:&str=include_str!("../fixtures/btc_one_market_minimal.csv");
    #[test]
    fn real_archived_oct24_yes_and_no_paper_ledger_arithmetic(){
        let rows=parse_bounded_csv(INPUT).unwrap();
        assert_eq!(rows.len(),2);
        assert_eq!(rows[0].ticker,"KXBTC15M-26OCT241645-45");
        let yes=reconcile_venue_reported_binary(&rows[0]).unwrap();
        let no=reconcile_venue_reported_binary(&rows[1]).unwrap();
        assert_eq!((yes.net_centicents,no.net_centicents),(4029,-4472));
        assert!(yes.matches_archived_arithmetic&&no.matches_archived_arithmetic);
        assert!(!yes.is_actual_venue_fill);
        assert!(!no.independent_settlement_verified);
    }
    #[test]fn rejects_precision_loss_and_unresolved_labels(){
        assert_eq!(money("0.00001"),Err(Error::InvalidDecimal));
        assert_eq!(money("0.402900").unwrap(),4029);
        let mut rows=parse_bounded_csv(INPUT).unwrap();
        rows[0].settlement_status="PENDING".into();
        assert_eq!(reconcile_venue_reported_binary(&rows[0]),Err(Error::UnresolvedOutcome));
    }
    #[test]fn rejects_changed_fee_and_unknown_record_type(){
        let mut row=parse_bounded_csv(INPUT).unwrap().remove(0);
        row.fee="0.0100".into();
        assert_eq!(reconcile_venue_reported_binary(&row),Err(Error::InconsistentLedger));
        row.record_type="LIVE".into();
        assert_eq!(reconcile_venue_reported_binary(&row),Err(Error::UnsupportedType));
    }
}
