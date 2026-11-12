//! Configurable settlement-index window arithmetic on caller-supplied samples.
//! This is NOT a claim about any specific contract's official sampling rules.
use phoenix_domain::Side;
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub struct IndexTick {pub second_utc:i64,pub value_1e8:i128}
#[derive(Debug,Clone,Copy)]
pub struct WindowRule {
    pub opening_start_sec:i64,
    pub closing_start_sec:i64,
    pub samples_per_window:u32,
    pub tie_resolves_yes:bool,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Error {InvalidRule,MissingSample,DuplicateSample,Overflow}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub struct WindowComparison {
    pub open_sum:i128,pub close_sum:i128,pub sample_count:u32,
    pub derived_winner:Side,
}
fn exact_window(ticks:&[IndexTick],start:i64,n:u32)->Result<i128,Error>{
    let mut total=0i128;
    for i in 0..n {
        let sec=start.checked_add(i64::from(i)).ok_or(Error::Overflow)?;
        let mut found=None;
        for tick in ticks.iter().filter(|t|t.second_utc==sec){
            if found.is_some(){return Err(Error::DuplicateSample);}
            found=Some(tick.value_1e8);
        }
        total=total.checked_add(found.ok_or(Error::MissingSample)?)
            .ok_or(Error::Overflow)?;
    }
    Ok(total)
}
/// Compare sums, not rounded display values. Only caller-supplied complete
/// reference observations can be used. Outcomes remain derived/unverified
/// until matched to contract rules and independent source provenance.
pub fn compare_window_averages(ticks:&[IndexTick],rule:WindowRule)
    ->Result<WindowComparison,Error>{
    if rule.samples_per_window==0 || rule.samples_per_window>3_600 ||
       rule.closing_start_sec < rule.opening_start_sec
          .checked_add(i64::from(rule.samples_per_window)).ok_or(Error::Overflow)? {
        return Err(Error::InvalidRule);
    }
    let open_sum=exact_window(ticks,rule.opening_start_sec,rule.samples_per_window)?;
    let close_sum=exact_window(ticks,rule.closing_start_sec,rule.samples_per_window)?;
    let derived_winner=if close_sum>open_sum ||
        (rule.tie_resolves_yes&&close_sum==open_sum){Side::Yes}else{Side::No};
    Ok(WindowComparison{open_sum,close_sum,
        sample_count:rule.samples_per_window,derived_winner})
}
#[cfg(test)]
mod tests{
    use super::*;
    fn rule()->WindowRule{WindowRule{opening_start_sec:0,closing_start_sec:60,
        samples_per_window:60,tie_resolves_yes:true}}
    fn fixture()->Vec<IndexTick>{
        (0..120).map(|i|IndexTick{second_utc:i,
            value_1e8:if i<60 {100_000_000}else{100_000_100}}).collect()
    }
    #[test] fn exact_60_second_open_close_comparison(){
        let result=compare_window_averages(&fixture(),rule()).unwrap();
        assert_eq!(result.sample_count,60);
        assert_eq!(result.derived_winner,Side::Yes);
        assert_eq!(result.close_sum-result.open_sum,6000);
    }
    #[test] fn missing_and_duplicate_ticks_cannot_be_interpolated(){
        let mut a=fixture();a.remove(40);
        assert_eq!(compare_window_averages(&a,rule()),Err(Error::MissingSample));
        let mut b=fixture();b.push(b[35]);
        assert_eq!(compare_window_averages(&b,rule()),Err(Error::DuplicateSample));
    }
    #[test] fn equality_respects_explicit_contract_rule(){
        let ticks=(0..120).map(|i|IndexTick{second_utc:i,value_1e8:100}).collect::<Vec<_>>();
        assert_eq!(compare_window_averages(&ticks,rule()).unwrap().derived_winner,Side::Yes);
        let mut no_on_tie=rule();no_on_tie.tie_resolves_yes=false;
        assert_eq!(compare_window_averages(&ticks,no_on_tie).unwrap().derived_winner,Side::No);
    }
    #[test] fn overlapping_windows_rejected(){
        let mut r=rule();r.closing_start_sec=59;
        assert_eq!(compare_window_averages(&fixture(),r),Err(Error::InvalidRule));
    }
}