import os
import shutil
import re
import pandas as pd
import wrds
from sqlalchemy import text

# ==========================================
# 1. CONFIGURATION
# ==========================================
TARGET_DIR = "./Datasets/compustat/fundamentals_quarterly_all"
START_YEAR = 2004
END_YEAR = 2026
os.makedirs(TARGET_DIR, exist_ok=True)

# The massive list of fields
RAW_FIELD_TEXT = """
(conm) Company Name
(tic) Ticker Symbol
(cusip) CUSIP
(cik) CIK Number
(exchg) Stock Exchange Code
(fyr) Fiscal Year-end Month
(fic) Current ISO Country Code - Incorporation
(add1) Address Line 1
(add2) Address Line 2
(add3) Address Line 3
(add4) Address Line 4
(addzip) Postal Code
(busdesc) S&P Business Description
(city) City
(conml) Company Legal Name
(county) County Code
(dldte) Research Company Deletion Date
(dlrsn) Research Co Reason for Deletion
(ein) Employer Identification Number
(fax) Fax Number
(fyrc) Current Fiscal Year End Month
(ggroup) GIC Groups
(gind) GIC Industries
(gsector) GIC Sectors
(gsubind) GIC Sub-Industries
(idbflag) International, Domestic, Both Indicator
(incorp) Current State/Province of Incorporation Code
(ipodate) Company Initial Public Offering Date
(loc) Current ISO Country Code - Headquarters
(naics) North American Industry Classification Code
(phone) Phone Number
(prican) Current Primary Issue Tag - Canada
(prirow) Primary Issue Tag - Rest of World
(priusa) Current Primary Issue Tag - US
(sic) Standard Industry Classification Code
(spcindcd) S&P Industry Sector Code
(spcseccd) S&P Economic Sector Code
(spcsrc) S&P Quality Ranking - Current
(state) State/Province
(stko) Stock Ownership Code
(weburl) Web URL
(acctchgq) Adoption of Accounting Changes
(acctstdq) Accounting Standard
(adrrq) ADR Ratio
(ajexq) Adjustment Factor (Company) - Cumulative by Ex-Date
(ajpq) Adjustment Factor (Company) - Cumulative byPay-Date
(apdedateq) Actual Period End date
(bsprq) Balance Sheet Presentation
(compstq) Comparability Status
(curncdq) Native Currency Code
(currtrq) Currency Translation Rate
(curuscnq) US Canadian Translation Rate - Interim
(datacqtr) Calendar Data Year and Quarter
(datafqtr) Fiscal Data Year and Quarter
(fdateq) Final Date
(finalq) Final Indicator Flag
(fqtr) Fiscal Quarter
(fyearq) Fiscal Year
(ogmq) OIL & GAS METHOD
(pdateq) Preliminary Date
(rdq) Report Date of Quarterly Earnings
(rp) Reporting Periodicity
(scfq) Cash Flow Model
(srcq) Source Code
(staltq) Status Alert
(updq) Update Code
(acchgq) Accounting Changes - Cumulative Effect
(acomincq) Accumulated Other Comprehensive Income (Loss)
(acoq) Current Assets - Other - Total
(actq) Current Assets - Total
(altoq) Other Long-term Assets
(ancq) Non-Current Assets - Total
(anoq) Assets Netting & Other Adjustments
(aociderglq) Accum Other Comp Inc - Derivatives Unrealized Gain/Loss
(aociotherq) Accum Other Comp Inc - Other Adjustments
(aocipenq) Accum Other Comp Inc - Min Pension Liab Adj
(aocisecglq) Accum Other Comp Inc - Unreal G/L Ret Int in Sec Assets
(aol2q) Assets Level2 (Observable)
(aoq) Assets - Other - Total
(apq) Account Payable/Creditors - Trade
(aqaq) Acquisition/Merger After-Tax
(aqdq) Acquisition/Merger Diluted EPS Effect
(aqepsq) Acquisition/Merger Basic EPS Effect
(aqpl1q) Assets Level1 (Quoted Prices)
(aqpq) Acquisition/Merger Pretax
(arcedq) As Reported Core - Diluted EPS Effect
(arceepsq) As Reported Core - Basic EPS Effect
(arceq) As Reported Core - After-tax
(atq) Assets - Total
(aul3q) Assets Level3 (Unobservable)
(billexceq) Billings in Excess of Cost & Earnings
(capr1q) Risk-Adjusted Capital Ratio - Tier 1
(capr2q) Risk-Adjusted Capital Ratio - Tier 2
(capr3q) Risk-Adjusted Capital Ratio - Combined
(capsftq) Capitalized Software
(capsq) Capital Surplus/Share Premium Reserve
(ceiexbillq) Cost & Earnings in Excess of Billings
(ceqq) Common/Ordinary Equity - Total
(cheq) Cash and Short-Term Investments
(chq) Cash
(cibegniq) Comp Inc - Beginning Net Income
(cicurrq) Comp Inc - Currency Trans Adj
(ciderglq) Comp Inc - Derivative Gains/Losses
(cimiiq) Comprehensive Income - Noncontrolling Interest
(ciotherq) Comp Inc - Other Adj
(cipenq) Comp Inc - Minimum Pension Adj
(ciq) Comprehensive Income - Total
(cisecglq) Comp Inc - Securities Gains/Losses
(citotalq) Comprehensive Income - Parent
(cogsq) Cost of Goods Sold
(csh12q) Common Shares Used to Calculate Earnings Per Share - 12 Months Moving
(cshfd12) Common Shares Used to Calc Earnings Per Share - Fully Diluted - 12 Months Moving
(cshfdq) Com Shares for Diluted EPS
(cshiq) Common Shares Issued
(cshopq) Total Shares Repurchased - Quarter
(cshoq) Common Shares Outstanding
(cshprq) Common Shares Used to Calculate Earnings Per Share - Basic
(cstkcvq) Carrying Value
(cstkeq) Common Stock Equivalents - Dollar Savings
(cstkq) Common/Ordinary Stock (Capital)
(dcomq) Deferred Compensation
(dd1q) Long-Term Debt Due in One Year
(deracq) Derivative Assets - Current
(deraltq) Derivative Assets Long-Term
(derhedglq) Gains/Losses on Derivatives and Hedging
(derlcq) Derivative Liabilities- Current
(derlltq) Derivative Liabilities Long-Term
(diladq) Dilution Adjustment
(dilavq) Dilution Available - Excluding Extraordinary Items
(dlcq) Debt in Current Liabilities
(dlttq) Long-Term Debt - Total
(doq) Discontinued Operations
(dpacreq) Accumulated Depreciation of RE Property
(dpactq) Depreciation, Depletion and Amortization (Accumulated)
(dpq) Depreciation and Amortization - Total
(dpretq) Depr/Amort of Property
(drcq) Deferred Revenue - Current
(drltq) Deferred Revenue - Long-term
(dteaq) Extinguishment of Debt After-tax
(dtedq) Extinguishment of Debt Diluted EPS Effect
(dteepsq) Extinguishment of Debt Basic EPS Effect
(dtepq) Extinguishment of Debt Pretax
(dvintfq) Dividends & Interest Receivable (Cash Flow)
(dvpq) Dividends - Preferred/Preference
(epsf12) Earnings Per Share (Diluted) - Excluding Extraordinary Items - 12 Months Moving
(epsfi12) Earnings Per Share (Diluted) - Including Extraordinary Items
(epsfiq) Earnings Per Share (Diluted) - Including Extraordinary Items
(epsfxq) Earnings Per Share (Diluted) - Excluding Extraordinary items
(epspi12) Earnings Per Share (Basic) - Including Extraordinary Items - 12 Months Moving
(epspiq) Earnings Per Share (Basic) - Including Extraordinary Items
(epspxq) Earnings Per Share (Basic) - Excluding Extraordinary Items
(epsx12) Earnings Per Share (Basic) - Excluding Extraordinary Items - 12 Months Moving
(esopctq) Common ESOP Obligation - Total
(esopnrq) Preferred ESOP Obligation - Non-Redeemable
(esoprq) Preferred ESOP Obligation - Redeemable
(esoptq) Preferred ESOP Obligation - Total
(esubq) Equity in Earnings (I/S) - Unconsolidated Subsidiaries
(fcaq) Foreign Exchange Income (Loss)
(ffoq) Funds From Operations (REIT)
(finacoq) Finance Division Other Current Assets, Total
(finaoq) Finance Division Other Long-Term Assets, Total
(finchq) Finance Division - Cash
(findlcq) Finance Division Long-Term Debt Current
(findltq) Finance Division Debt Long-Term
(finivstq) Finance Division Short-Term Investments
(finlcoq) Finance Division Other Current Liabilities, Total
(finltoq) Finance Division Other Long Term Liabilities, Total
(finnpq) Finance Division Notes Payable
(finreccq) Finance Division Current Receivables
(finrecltq) Finance Division Long-Term Receivables
(finrevq) Finance Division Revenue
(finxintq) Finance Division Interest Expense
(finxoprq) Finance Division Operating Expense
(fyr) Fiscal Year-end Month
(gdwlamq) Amortization of Goodwill
(gdwlia12) Impairments of Goodwill AfterTax - 12mm
(gdwliaq) Impairment of Goodwill After-tax
(gdwlid12) Impairments Diluted EPS - 12mm
(gdwlidq) Impairment of Goodwill Diluted EPS Effect
(gdwlieps12) Impairment of Goodwill Basic EPS Effect 12MM
(gdwliepsq) Impairment of Goodwill Basic EPS Effect
(gdwlipq) Impairment of Goodwill Pretax
(gdwlq) Goodwill (net)
(glaq) Gain/Loss After-Tax
(glcea12) Gain/Loss on Sale (Core Earnings Adjusted) After-tax 12MM
(glceaq) Gain/Loss on Sale (Core Earnings Adjusted) After-tax
(glced12) Gain/Loss on Sale (Core Earnings Adjusted) Diluted EPS Effect 12MM
(glcedq) Gain/Loss on Sale (Core Earnings Adjusted) Diluted EPS
(glceeps12) Gain/Loss on Sale (Core Earnings Adjusted) Basic EPS Effect 12MM
(glceepsq) Gain/Loss on Sale (Core Earnings Adjusted) Basic EPS Effect
(glcepq) Gain/Loss on Sale (Core Earnings Adjusted) Pretax
(gldq) Gain/Loss Diluted EPS Effect
(glepsq) Gain/Loss Basic EPS Effect
(glivq) Gains/Losses on investments
(glpq) Gain/Loss Pretax
(hedgeglq) Gain/Loss on Ineffective Hedges
(ibadj12) Income Before Extra Items - Adj for Common Stock Equivalents - 12MM
(ibadjq) Income Before Extraordinary Items - Adjusted for Common Stock Equivalents
(ibcomq) Income Before Extraordinary Items - Available for Common
(ibmiiq) Income before Extraordinary Items and Noncontrolling Interests
(ibq) Income Before Extraordinary Items
(icaptq) Invested Capital - Total - Quarterly
(intaccq) Interest Accrued
(intanoq) Other Intangibles
(intanq) Intangible Assets - Total
(invfgq) Inventory - Finished Goods
(invoq) Inventory - Other
(invrmq) Inventory - Raw Materials
(invtq) Inventories - Total
(invwipq) Inventory - Work in Process
(ivaeqq) Investment and Advances - Equity
(ivaoq) Investment and Advances - Other
(ivltq) Total Long-term Investments
(ivstq) Short-Term Investments- Total
(lcoq) Current Liabilities - Other - Total
(lctq) Current Liabilities - Total
(lltq) Long-Term Liabilities (Total)
(lnoq) Liabilities Netting & Other Adjustments
(lol2q) Liabilities Level2 (Observable)
(loq) Liabilities - Other
(loxdrq) Liabilities - Other - Excluding Deferred Revenue
(lqpl1q) Liabilities Level1 (Quoted Prices)
(lseq) Liabilities and Stockholders Equity - Total
(ltmibq) Liabilities - Total and Noncontrolling Interest
(ltq) Liabilities - Total
(lul3q) Liabilities Level3 (Unobservable)
(mibnq) Noncontrolling Interests - Nonredeemable - Balance Sheet
(mibq) Noncontrolling Interest - Redeemable - Balance Sheet
(mibtq) Noncontrolling Interests - Total - Balance Sheet
(miiq) Noncontrolling Interest - Income Account
(msaq) Accum Other Comp Inc - Marketable Security Adjustments
(ncoq) Net Charge-Offs
(niitq) Net Interest Income (Tax Equivalent)
(nimq) Net Interest Margin
(niq) Net Income (Loss)
(nopiq) Non-Operating Income (Expense) - Total
(npatq) Nonperforming Assets - Total
(npq) Notes Payable
(nrtxtdq) Nonrecurring Income Taxes Diluted EPS Effect
(nrtxtepsq) Nonrecurring Income Taxes Basic EPS Effect
(nrtxtq) Nonrecurring Income Taxes - After-tax
(obkq) Order backlog
(oepf12) Earnings Per Share - Diluted - from Operations - 12MM
(oeps12) Earnings Per Share from Operations - 12 Months Moving
(oepsxq) Earnings Per Share - Diluted - from Operations
(oiadpq) Operating Income After Depreciation - Quarterly
(oibdpq) Operating Income Before Depreciation - Quarterly
(opepsq) Earnings Per Share from Operations
(optdrq) Dividend Rate - Assumption (%)
(optfvgrq) Options - Fair Value of Options Granted
(optlifeq) Life of Options - Assumption (# yrs)
(optrfrq) Risk Free Rate - Assumption (%)
(optvolq) Volatility - Assumption (%)
(piq) Pretax Income
(pllq) Provision for Loan/Asset Losses
(pnc12) Pension Core Adjustment - 12mm
(pncd12) Core Pension Adjustment Diluted EPS Effect 12MM
(pncdq) Core Pension Adjustment Diluted EPS Effect
(pnceps12) Core Pension Adjustment Basic EPS Effect 12MM
(pncepsq) Core Pension Adjustment Basic EPS Effect
(pnciapq) Core Pension Interest Adjustment After-tax Preliminary
(pnciaq) Core Pension Interest Adjustment After-tax
(pncidpq) Core Pension Interest Adjustment Diluted EPS Effect Preliminary
(pncidq) Core Pension Interest Adjustment Diluted EPS Effect
(pnciepspq) Core Pension Interest Adjustment Basic EPS Effect Preliminary
(pnciepsq) Core Pension Interest Adjustment Basic EPS Effect
(pncippq) Core Pension Interest Adjustment Pretax Preliminary
(pncipq) Core Pension Interest Adjustment Pretax
(pncpd12) Core Pension Adjustment 12MM Diluted EPS Effect Preliminary
(pncpdq) Core Pension Adjustment Diluted EPS Effect Preliminary
(pncpeps12) Core Pension Adjustment 12MM Basic EPS Effect Preliminary
(pncpepsq) Core Pension Adjustment Basic EPS Effect Preliminary
(pncpq) Core Pension Adjustment Preliminary
(pncq) Core Pension Adjustment
(pncwiapq) Core Pension w/o Interest Adjustment After-tax Preliminary
(pncwiaq) Core Pension w/o Interest Adjustment After-tax
(pncwidpq) Core Pension w/o Interest Adjustment Diluted EPS Effect Preliminary
(pncwidq) Core Pension w/o Interest Adjustment Diluted EPS Effect
(pncwiepq) Core Pension w/o Interest Adjustment Basic EPS Effect Preliminary
(pncwiepsq) Core Pension w/o Interest Adjustment Basic EPS Effect
(pncwippq) Core Pension w/o Interest Adjustment Pretax Preliminary
(pncwipq) Core Pension w/o Interest Adjustment Pretax
(pnrshoq) Nonred Pfd Shares Outs (000) - Quarterly
(ppegtq) Property, Plant and Equipment - Total (Gross) - Quarterly
(ppentq) Property Plant and Equipment - Total (Net)
(prcaq) Core Post Retirement Adjustment
(prcd12) Core Post Retirement Adjustment Diluted EPS Effect 12MM
(prcdq) Core Post Retirement Adjustment Diluted EPS Effect
(prce12) Core Post Retirement Adjustment 12MM
(prceps12) Core Post Retirement Adjustment Basic EPS Effect 12MM
(prcepsq) Core Post Retirement Adjustment Basic EPS Effect
(prcpd12) Core Post Retirement Adjustment 12MM Diluted EPS Effect Preliminary
(prcpdq) Core Post Retirement Adjustment Diluted EPS Effect Preliminary
(prcpeps12) Core Post Retirement Adjustment 12MM Basic EPS Effect Preliminary
(prcpepsq) Core Post Retirement Adjustment Basic EPS Effect Preliminary
(prcpq) Core Post Retirement Adjustment Preliminary
(prcraq) Repurchase Price - Average per share Quarter
(prshoq) Redeem Pfd Shares Outs (000)
(pstknq) Preferred/Preference Stock - Nonredeemable
(pstkq) Preferred/Preference Stock (Capital) - Total
(pstkrq) Preferred/Preference Stock - Redeemable
(rcaq) Restructuring Cost After-tax
(rcdq) Restructuring Cost Diluted EPS Effect
(rcepsq) Restructuring Cost Basic EPS Effect
(rcpq) Restructuring Cost Pretax
(rdipaq) In Process R&D Expense After-tax
(rdipdq) In Process R&D Expense Diluted EPS Effect
(rdipepsq) In Process R&D Expense Basic EPS Effect
(rdipq) In Process R&D
(recdq) Receivables - Estimated Doubtful
(rectaq) Accum Other Comp Inc - Cumulative Translation Adjustments
(rectoq) Receivables - Current Other incl Tax Refunds
(rectq) Receivables - Total
(rectrq) Receivables - Trade
(recubq) Unbilled Receivables - Quarterly
(req) Retained Earnings
(retq) Total RE Property
(reunaq) Unadjusted Retained Earnings
(revtq) Revenue - Total
(rllq) Reserve for Loan/Asset Losses
(rra12) Reversal - Restructruring/Acquisition Aftertax 12MM
(rraq) Reversal - Restructruring/Acquisition Aftertax
(rrd12) Reversal - Restructuring/Acq Diluted EPS Effect 12MM
(rrdq) Reversal - Restructuring/Acq Diluted EPS Effect
(rreps12) Reversal - Restructuring/Acq Basic EPS Effect 12MM
(rrepsq) Reversal - Restructuring/Acq Basic EPS Effect
(rrpq) Reversal - Restructruring/Acquisition Pretax
(rstcheltq) Long-Term Restricted Cash & Investments
(rstcheq) Restricted Cash & Investments - Current
(saleq) Sales/Turnover (Net)
(seqoq) Other Stockholders- Equity Adjustments
(seqq) Stockholders Equity > Parent > Index Fundamental > Quarterly
(seta12) Settlement (Litigation/Insurance) AfterTax - 12mm
(setaq) Settlement (Litigation/Insurance) After-tax
(setd12) Settlement (Litigation/Insurance) Diluted EPS Effect 12MM
(setdq) Settlement (Litigation/Insurance) Diluted EPS Effect
(seteps12) Settlement (Litigation/Insurance) Basic EPS Effect 12MM
(setepsq) Settlement (Litigation/Insurance) Basic EPS Effect
(setpq) Settlement (Litigation/Insurance) Pretax
(spce12) S&P Core Earnings 12MM
(spced12) S&P Core Earnings EPS Diluted 12MM
(spcedpq) S&P Core Earnings EPS Diluted - Preliminary
(spcedq) S&P Core Earnings EPS Diluted
(spceeps12) S&P Core Earnings EPS Basic 12MM
(spceepsp12) S&P Core 12MM EPS - Basic - Preliminary
(spceepspq) S&P Core Earnings EPS Basic - Preliminary
(spceepsq) S&P Core Earnings EPS Basic
(spcep12) S&P Core Earnings 12MM - Preliminary
(spcepd12) S&P Core Earnings 12MM EPS Diluted - Preliminary
(spcepq) S&P Core Earnings - Preliminary
(spceq) S&P Core Earnings
(spidq) Other Special Items Diluted EPS Effect
(spiepsq) Other Special Items Basic EPS Effect
(spioaq) Other Special Items After-tax
(spiopq) Other Special Items Pretax
(spiq) Special Items
(sretq) Gain/Loss on Sale of Property
(stkcoq) Stock Compensation Expense
(stkcpaq) After-tax stock compensation
(teqq) Stockholders Equity - Total
(tfvaq) Total Fair Value Assets
(tfvceq) Total Fair Value Changes including Earnings
(tfvlq) Total Fair Value Liabilities
(tieq) Interest Expense - Total (Financial Services)
(tiiq) Interest Income - Total (Financial Services)
(tstknq) Treasury Stock - Number of Common Shares
(tstkq) Treasury Stock - Total (All Capital)
(txdbaq) Deferred Tax Asset - Long Term
(txdbcaq) Current Deferred Tax Asset
(txdbclq) Current Deferred Tax Liability
(txdbq) Deferred Taxes - Balance Sheet
(txdiq) Income Taxes - Deferred
(txditcq) Deferred Taxes and Investment Tax Credit
(txpq) Income Taxes Payable
(txtq) Income Taxes - Total
(txwq) Excise Taxes
(uacoq) Current Assets - Other - Utility
(uaoq) Other Assets - Utility
(uaptq) Accounts Payable - Utility
(ucapsq) Paid In Capital - Other - Utility
(ucconsq) Contributions In Aid Of Construction
(uceqq) Common Equity - Total - Utility
(uddq) Debt (Debentures) - Utility
(udmbq) Debt (Mortgage Bonds)
(udoltq) Debt (Other Long-Term)
(udpcoq) Debt (Pollution Control Obligations)
(udvpq) Preferred Dividend Requirements
(ugiq) Gross Income (Income Before Interest Charges)
(uinvq) Inventories
(ulcoq) Current Liabilities - Other
(uniamiq) Net Income before Extraordinary Items After Noncontrolling Interest
(unopincq) Nonoperating Income (Net) - Other
(uopiq) Operating Income - Total - Utility
(updvpq) Preference Dividend Requirements - Utility
(upmcstkq) Premium On Common Stock - Utility
(upmpfq) Premium On Preferred Stock - Utility
(upmpfsq) Premium On Preference Stock - Utility
(upmsubpq) Premium On Subsidiary Preferred Stock - Utility
(upstkcq) Preference Stock At Carrying Value - Utility
(upstkq) Preferred Stock At Carrying Value - Utility
(urectq) Receivables (Net) - Utility
(uspiq) Special Items - Utility
(usubdvpq) Subsidiary Preferred Dividends - Utility
(usubpcvq) Subsidiary Preferred Stock At Carrying Value - Utility
(utemq) Maintenance Expense - Total
(wcapq) Working Capital (Balance Sheet)
(wdaq) Writedowns After-tax
(wddq) Writedowns Diluted EPS Effect
(wdepsq) Writedowns Basic EPS Effect
(wdpq) Writedowns Pretax
(xaccq) Accrued Expenses
(xidoq) Extraordinary Items and Discontinued Operations
(xintq) Interest and Related Expense- Total
(xiq) Extraordinary Items
(xoprq) Operating Expense- Total
(xopt12) Implied Option Expense - 12mm
(xoptd12) Implied Option EPS Diluted 12MM
(xoptd12p) Implied Option 12MM EPS Diluted Preliminary
(xoptdq) Implied Option EPS Diluted
(xoptdqp) Implied Option EPS Diluted Preliminary
(xopteps12) Implied Option EPS Basic 12MM
(xoptepsp12) Implied Option 12MM EPS Basic Preliminary
(xoptepsq) Implied Option EPS Basic
(xoptepsqp) Implied Option EPS Basic Preliminary
(xoptq) Implied Option Expense
(xoptqp) Implied Option Expense Preliminary
(xrdq) Research and Development Expense
(xsgaq) Selling, General and Administrative Expenses
ISO Currency Code (curcdq)
(acchgy) Accounting Changes - Cumulative Effect
(afudccy) Allowance for Funds Used During Construction (Cash Flow)
(afudciy) Allowance for Funds Used During Construction (Investing) (Cash Flow)
(amcy) Amortization (Cash Flow)
(aolochy) Assets and Liabilities - Other (Net Change)
(apalchy) Accounts Payable and Accrued Liabilities - Increase (Decrease)
(aqay) Acquisition/Merger After-Tax
(aqcy) Acquisitions
(aqdy) Acquisition/Merger Diluted EPS Effect
(aqepsy) Acquisition/Merger Basic EPS Effect
(aqpy) Acquisition/Merger Pretax
(arcedy) As Reported Core - Diluted EPS Effect
(arceepsy) As Reported Core - Basic EPS Effect
(arcey) As Reported Core - After-tax
(capxy) Capital Expenditures
(cdvcy) Cash Dividends on Common Stock (Cash Flow)
(chechy) Cash and Cash Equivalents - Increase (Decrease)
(cibegniy) Comp Inc - Beginning Net Income
(cicurry) Comp Inc - Currency Trans Adj
(cidergly) Comp Inc - Derivative Gains/Losses
(cimiiy) Comprehensive Income - Noncontrolling Interest
(ciothery) Comp Inc - Other Adj
(cipeny) Comp Inc - Minimum Pension Adj
(cisecgly) Comp Inc - Securities Gains/Losses
(citotaly) Comprehensive Income - Parent
(ciy) Comprehensive Income - Total
(cogsy) Cost of Goods Sold
(cshfdy) Com Shares for Diluted EPS
(cshpry) Common Shares Used to Calculate Earnings Per Share - Basic
(cstkey) Common Stock Equivalents - Dollar Savings
(depcy) Depreciation and Depletion (Cash Flow)
(derhedgly) Gains/Losses on Derivatives and Hedging
(dilady) Dilution Adjustment
(dilavy) Dilution Available - Excluding Extraordinary Items
(dlcchy) Changes in Current Debt
(dltisy) Long-Term Debt - Issuance
(dltry) Long-Term Debt - Reduction
(doy) Discontinued Operations
(dpcy) Depreciation and Amortization - Statement of Cash Flows
(dprety) Depr/Amort of Property
(dpy) Depreciation and Amortization - Total
(dteay) Extinguishment of Debt After-tax
(dtedy) Extinguishment of Debt Diluted EPS Effect
(dteepsy) Extinguishment of Debt Basic EPS Effect
(dtepy) Extinguishment of Debt Pretax
(dvpy) Dividends - Preferred/Preference
(dvy) Cash Dividends
(epsfiy) Earnings Per Share (Diluted) - Including Extraordinary Items
(epsfxy) Earnings Per Share (Diluted) - Excluding Extraordinary items
(epspiy) Earnings Per Share (Basic) - Including Extraordinary Items
(epspxy) Earnings Per Share (Basic) - Excluding Extraordinary Items
(esubcy) Equity in Net Loss/Earnings (C/F)
(esuby) Equity in Earnings (I/S)- Unconsolidated Subsidiaries
(exrey) Exchange Rate Effect
(fcay) Foreign Exchange Income (Loss)
(ffoy) Funds From Operations (REIT)
(fiaoy) Financing Activities - Other
(fincfy) Financing Activities - Net Cash Flow
(finrevy) Finance Division Revenue
(finxinty) Finance Division Interest Expense
(finxopry) Finance Division Operating Expense
(fopoxy) Funds from Operations - Other excluding Option Tax Benefit
(fopoy) Funds from Operations - Other
(fopty) Funds From Operations - Total
(fsrcoy) Sources of Funds - Other
(fsrcty) Sources of Funds - Total
(fuseoy) Uses of Funds - Other
(fusety) Uses of Funds - Total
(fyr) Fiscal Year-end Month
(gdwlamy) Amortization of Goodwill
(gdwliay) Impairment of Goodwill After-tax
(gdwlidy) Impairment of Goodwill Diluted EPS Effect
(gdwliepsy) Impairment of Goodwill Basic EPS Effect
(gdwlipy) Impairment of Goodwill Pretax
(glay) Gain/Loss After-Tax
(glceay) Gain/Loss on Sale (Core Earnings Adjusted) After-tax
(glcedy) Gain/Loss on Sale (Core Earnings Adjusted) Diluted EPS
(glceepsy) Gain/Loss on Sale (Core Earnings Adjusted) Basic EPS Effect
(glcepy) Gain/Loss on Sale (Core Earnings Adjusted) Pretax
(gldy) Gain/Loss Diluted EPS Effect
(glepsy) Gain/Loss Basic EPS Effect
(glivy) Gains/Losses on investments
(glpy) Gain/Loss Pretax
(hedgegly) Gain/Loss on Ineffective Hedges
(ibadjy) Income Before Extraordinary Items - Adjusted for Common Stock Equivalents
(ibcomy) Income Before Extraordinary Items - Available for Common
(ibcy) Income Before Extraordinary Items - Statement of Cash Flows
(ibmiiy) Income before Extraordinary Items and Noncontrolling Interests
(iby) Income Before Extraordinary Items
(intpny) Interest Paid - Net
(invchy) Inventory - Decrease (Increase)
(itccy) Investment Tax Credit - Net (Cash Flow)
(ivacoy) Investing Activities - Other
(ivchy) Increase in Investments
(ivncfy) Investing Activities - Net Cash Flow
(ivstchy) Short-Term Investments - Change
(miiy) Noncontrolling Interest - Income Account
(ncoy) Net Charge-Offs
(niity) Net Interest Income (Tax Equivalent)
(nimy) Net Interest Margin
(niy) Net Income (Loss)
(nopiy) Non-Operating Income (Expense) - Total
(nrtxtdy) Nonrecurring Income Taxes Diluted EPS Effect
(nrtxtepsy) Nonrecurring Income Taxes Basic EPS Effect
(nrtxty) Nonrecurring Income Taxes - After-tax
(oancfy) Operating Activities - Net Cash Flow
(oepsxy) Earnings Per Share - Diluted - from Operations
(oiadpy) Operating Income After Depreciation - Year-to-Date
(oibdpy) Operating Income Before Depreciation
(opepsy) Earnings Per Share from Operations
(optdry) Dividend Rate - Assumption (%)
(optfvgry) Options - Fair Value of Options Granted
(optlifey) Life of Options - Assumption (# yrs)
(optrfry) Risk Free Rate - Assumption (%)
(optvoly) Volatility - Assumption (%)
(pdvcy) Cash Dividends on Preferred/Preference Stock (Cash Flow)
(piy) Pretax Income
(plly) Provision for Loan/Asset Losses
(pncdy) Core Pension Adjustment Diluted EPS Effect
(pncepsy) Core Pension Adjustment Basic EPS Effect
(pnciapy) Core Pension Interest Adjustment After-tax Preliminary
(pnciay) Core Pension Interest Adjustment After-tax
(pncidpy) Core Pension Interest Adjustment Diluted EPS Effect Preliminary
(pncidy) Core Pension Interest Adjustment Diluted EPS Effect
(pnciepspy) Core Pension Interest Adjustment Basic EPS Effect Preliminary
(pnciepsy) Core Pension Interest Adjustment Basic EPS Effect
(pncippy) Core Pension Interest Adjustment Pretax Preliminary
(pncipy) Core Pension Interest Adjustment Pretax
(pncpdy) Core Pension Adjustment Diluted EPS Effect Preliminary
(pncpepsy) Core Pension Adjustment Basic EPS Effect Preliminary
(pncpy) Core Pension Adjustment Preliminary
(pncwiapy) Core Pension w/o Interest Adjustment After-tax Preliminary
(pncwiay) Core Pension w/o Interest Adjustment After-tax
(pncwidpy) Core Pension w/o Interest Adjustment Diluted EPS Effect Preliminary
(pncwidy) Core Pension w/o Interest Adjustment Diluted EPS Effect
(pncwiepsy) Core Pension w/o Interest Adjustment Basic EPS Effect
(pncwiepy) Core Pension w/o Interest Adjustment Basic EPS Effect Preliminary
(pncwippy) Core Pension w/o Interest Adjustment Pretax Preliminary
(pncwipy) Core Pension w/o Interest Adjustment Pretax
(pncy) Core Pension Adjustment
(prcay) Core Post Retirement Adjustment
(prcdy) Core Post Retirement Adjustment Diluted EPS Effect
(prcepsy) Core Post Retirement Adjustment Basic EPS Effect
(prcpdy) Core Post Retirement Adjustment Diluted EPS Effect Preliminary
(prcpepsy) Core Post Retirement Adjustment Basic EPS Effect Preliminary
(prcpy) Core Post Retirement Adjustment Preliminary
(prstkccy) Purchase of Common Stock (Cash Flow)
(prstkcy) Purchase of Common and Preferred Stock
(prstkpcy) Purchase of Preferred/Preference Stock (Cash Flow)
(rcay) Restructuring Cost After-tax
(rcdy) Restructuring Cost Diluted EPS Effect
(rcepsy) Restructuring Cost Basic EPS Effect
(rcpy) Restructuring Cost Pretax
(rdipay) In Process R&D Expense After-tax
(rdipdy) In Process R&D Expense Diluted EPS Effect
(rdipepsy) In Process R&D Expense Basic EPS Effect
(rdipy) In Process R&D
(recchy) Accounts Receivable - Decrease (Increase)
(revty) Revenue - Total
(rray) Reversal - Restructruring/Acquisition Aftertax
(rrdy) Reversal - Restructuring/Acq Diluted EPS Effect
(rrepsy) Reversal - Restructuring/Acq Basic EPS Effect
(rrpy) Reversal - Restructruring/Acquisition Pretax
(saley) Sales/Turnover (Net)
(scstkcy) Sale of Common Stock (Cash Flow)
(setay) Settlement (Litigation/Insurance) After-tax
(setdy) Settlement (Litigation/Insurance) Diluted EPS Effect
(setepsy) Settlement (Litigation/Insurance) Basic EPS Effect
(setpy) Settlement (Litigation/Insurance) Pretax
(sivy) Sale of Investments
(spcedpy) S&P Core Earnings EPS Diluted - Preliminary
(spcedy) S&P Core Earnings EPS Diluted
(spceepspy) S&P Core Earnings EPS Basic - Preliminary
(spceepsy) S&P Core Earnings EPS Basic
(spcepy) S&P Core Earnings - Preliminary
(spcey) S&P Core Earnings
(spidy) Other Special Items Diluted EPS Effect
(spiepsy) Other Special Items Basic EPS Effect
(spioay) Other Special Items After-tax
(spiopy) Other Special Items Pretax
(spiy) Special Items
(sppey) Sale of Property
(sppivy) Sale of PP&E and Investments - (Gain) Loss
(spstkcy) Sale of Preferred/Preference Stock (Cash Flow)
(srety) Gain/Loss on Sale of Property
(sstky) Sale of Common and Preferred Stock
(stkcoy) Stock Compensation Expense
(stkcpay) After-tax stock compensation
(tdcy) Deferred Income Taxes - Net (Cash Flow)
(tfvcey) Total Fair Value Changes including Earnings
(tiey) Interest Expense - Total (Financial Services)
(tiiy) Interest Income - Total (Financial Services)
(tsafcy) Total Srcs of Funds (FOF)
(txachy) Income Taxes - Accrued - Increase (Decrease)
(txbcofy) Excess Tax Benefit of Stock Options - Cash Flow Financing
(txbcoy) Excess Tax Benefit of Stock Options - Cash Flow Operating
(txdcy) Deferred Taxes (Statement of Cash Flows)
(txdiy) Income Taxes - Deferred
(txpdy) Income Taxes Paid
(txty) Income Taxes - Total
(txwy) Excise Taxes
(uaolochy) Other Assets and Liabilities - Net Change (Statement of Cash Flows)
(udfccy) Deferred Fuel - Increase (Decrease) (Statement of Cash Flows)
(udvpy) Preferred Dividend Requirements - Utility
(ufretsdy) Tot Funds Ret ofSec&STD (FOF)
(ugiy) Gross Income (Income Before Interest Charges) - Utility
(uniamiy) Net Income before Extraordinary Items After Noncontrolling Interest - Utility
(unopincy) Nonoperating Income (Net) - Other - Utility
(unwccy) Inc(Dec)Working Cap (FOF)
(uoisy) Other Internal Sources - Net (Cash Flow)
(updvpy) Preference Dividend Requirements - Utility
(uptacy) Utility Plant - Gross Additions (Cash Flow)
(uspiy) Special Items - Utility
(ustdncy) Net Decr in ST Debt (FOF)
(usubdvpy) Subsidiary Preferred Dividends - Utility
(utfdocy) Total Funds From Ops (FOF)
(utfoscy) Tot Funds Frm Outside Sources (FOF)
(utmey) Maintenance Expense - Total
(uwkcapcy) Dec(Inc) in Working Capital (FOF)
(wcapchy) Working Capital Changes - Total
(wcapcy) Working Capital Change - Other - Increase/(Decrease)
(wday) Writedowns After-tax
(wddy) Writedowns Diluted EPS Effect
(wdepsy) Writedowns Basic EPS Effect
(wdpy) Writedowns Pretax
(xidocy) Extraordinary Items and Discontinued Operations (Statement of Cash Flows)
(xidoy) Extraordinary Items and Discontinued Operations
(xinty) Interest and Related Expense- Total
(xiy) Extraordinary Items
(xopry) Operating Expense- Total
(xoptdqpy) Implied Option EPS Diluted Preliminary
(xoptdy) Implied Option EPS Diluted
(xoptepsqpy) Implied Option EPS Basic Preliminary
(xoptepsy) Implied Option EPS Basic
(xoptqpy) Implied Option Expense Preliminary
(xopty) Implied Option Expense
(xrdy) Research and Development Expense
(xsgay) Selling, General and Administrative Expenses
(adjex) Cumulative Adjustment Factor by Ex-Date
(cshtrq) Common Shares Traded - Quarter
(dvpspq) Dividends per Share - Pay Date - Quarter
(dvpsxq) Div per Share - Exdate - Quarter
(mkvaltq) Market Value - Total
(prccq) Price Close - Quarter
(prchq) Price High - Quarter
(prclq) Price Low - Quarter
"""

# 2. EXTRACT FIELDS (Strict Regex)
# Only grabs the first parenthesis on each line to avoid descriptions
fields_raw = re.findall(r"^\s*\((.*?)\)", RAW_FIELD_TEXT, re.MULTILINE)
fields = list(set([f.strip() for f in fields_raw]))

# 3. CONNECT
db = wrds.Connection()

# 4. VALIDATE COLUMNS (Crucial Step)
# This checks which of your requested fields actually exist in comp.fundq
print("Validating columns against WRDS metadata...")
db_cols = db.describe_table('comp', 'fundq')
# Robustly extract column names from the describe_table result.
# Different versions or environments may name the column holding
# the column names differently (e.g. 'column_name', 'column', 'name').
if isinstance(db_cols, pd.DataFrame):
    if 'column_name' in db_cols.columns:
        col_series = db_cols['column_name']
    elif 'column' in db_cols.columns:
        col_series = db_cols['column']
    elif 'name' in db_cols.columns:
        col_series = db_cols['name']
    else:
        # Fallback: assume the first dataframe column contains the names
        col_series = db_cols.iloc[:, 0]
    valid_db_cols = set(col_series.astype(str).str.strip().values)
else:
    # If describe_table returned a list/dict-like object, try to coerce
    try:
        valid_db_cols = set(pd.Series(db_cols).astype(str).str.strip().values)
    except Exception:
        valid_db_cols = set()

print(f"Found {len(valid_db_cols)} columns in comp.fundq")

# Filter your list to only include what exists in the table
final_fields = [f for f in fields if f in valid_db_cols]

# Ensure mandatory keys are present
mandatory = ['gvkey', 'datadate', 'fyearq', 'fqtr', 'indfmt', 'datafmt', 'popsrc', 'consol']
for m in mandatory:
    if m not in final_fields:
        final_fields.insert(0, m)

print(f"Requested: {len(fields)} | Valid in fundq: {len(final_fields)}")
cols_sql = ",".join(final_fields)

# 5. FETCH DATA
years_list = range(START_YEAR, END_YEAR + 1)

for year in years_list:
    print(f"Fetching Fiscal Year: {year}...")
    # Use text() to bypass SQLAlchemy 2.0 'Connection has no cursor' error
    sql = text(f"SELECT {cols_sql} FROM comp.fundq WHERE fyearq = {year}")
    
    try:
        df = db.raw_sql(f"SELECT {cols_sql} FROM comp.fundq WHERE fyearq = {year}")
        
        if not df.empty:
            df['gvkey'] = df['gvkey'].astype(str)
            df['datadate'] = pd.to_datetime(df['datadate'])
            
            outfile = os.path.join(TARGET_DIR, f"fundamentals_quarterly_{year}.parquet")
            df.to_parquet(outfile, index=False)
            print(f"Successfully saved {year} ({len(df)} rows).")
        else:
            print(f"No data for {year}.")
            
    except Exception as e:
        print(f"Error processing {year}: {e}")

print("Done.")