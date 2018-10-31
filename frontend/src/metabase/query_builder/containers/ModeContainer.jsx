import React from "react";
import { Box, Flex } from "grid-styled";
import { Motion, spring } from "react-motion";
import cx from "classnames";
import { findDOMNode } from "react-dom";

import fitViewport from "metabase/hoc/FitViewPort";
import QuestionAndResultLoader from "metabase/containers/QuestionAndResultLoader";
import Visualization from "metabase/visualizations/components/Visualization";

import { Absolute, Relative } from "metabase/components/Position";
import Button from "metabase/components/Button";
import Card from "metabase/components/Card";
import Icon, { IconWrapper } from "metabase/components/Icon";

const HeaderControls = ({ onToggleReference, onToggleVisualization }) => (
  <Flex align="center">
    <Motion
      defaultStyle={{ scale: 0 }}
      style={{ scale: onToggleVisualization ? spring(1) : spring(0) }}
    >
      {({ scale }) => (
        <Button
          medium
          mx={2}
          primary
          onClick={() => onToggleVisualization()}
          style={{ transform: `scale(${scale})` }}
        >
          Show visualization
        </Button>
      )}
    </Motion>
    <IconWrapper onClick={() => onToggleReference()}>
      <Icon name="reference" />
    </IconWrapper>
  </Flex>
);

const PanelHeader = ({ children }) => <h3>{children}</h3>;

const PageWrapper = ({ children }) => (
  <Box p={2} className="relative">
    {children}
  </Box>
);

class ReferencePanel extends React.Component {
  render() {
    return (
      <Card p={2} style={{ width: 320 }}>
        <PanelHeader>Reference</PanelHeader>
      </Card>
    );
  }
}

class VisualizationPanel extends React.Component {
  render() {
    return (
      <Card p={2} style={{ width: 320 }}>
        <PanelHeader>Viz settings</PanelHeader>
      </Card>
    );
  }
}

class ResultPanel extends React.Component {
  render() {
    return (
      <Flex
        py={2}
        flexDirection="column"
        align="center"
        justify="center"
        className="border-top bg-white overflow-hidden"
      >
        <Icon name="table" mr={1} />
        View result table
        <Box className="relative" style={{ height: 400, width: "100%" }}>
          <QuestionAndResultLoader questionId={8}>
            {({ question, result, cancel, reload, rawSeries, loading }) => {
              return (
                <Box className="spread flex flex-column">
                  {rawSeries && <Visualization rawSeries={rawSeries} />}
                </Box>
              );
            }}
          </QuestionAndResultLoader>
        </Box>
        <Box className="relative" style={{ height: 400, width: "100%" }}>
          <QuestionAndResultLoader questionId={9}>
            {({ question, result, cancel, reload, rawSeries, loading }) => {
              return (
                <Box className="spread flex flex-column">
                  {rawSeries && <Visualization rawSeries={rawSeries} />}
                </Box>
              );
            }}
          </QuestionAndResultLoader>
        </Box>
      </Flex>
    );
  }
}

@fitViewport
class ModeContainer extends React.Component {
  state = {
    vizPanelOpen: false,
    referencePanelOpen: false,
    showResultPane: false,
  };
  render() {
    const { referencePanelOpen, vizPanelOpen, showResultPane } = this.state;
    const wrapperHeight =
      findDOMNode(this.container) && findDOMNode(this.container).offsetHeight;
    return (
      <Flex
        flexDirection="column"
        w={"100%"}
        className={cx(this.props.fitClassNames, "overflow-hidden")}
      >
        <QuestionAndResultLoader questionId={7}>
          {({ question, result, cancel, reload, rawSeries, loading }) => {
            if (!question) {
              return <div>"Loading..."</div>;
            }
            window.q = question;
            return (
              <Box className="flex flex-column flex-full">
                <Flex align="center" p={3}>
                  <h2>{question.displayName()}</h2>
                  <Box ml="auto">
                    <HeaderControls
                      onToggleReference={() =>
                        this.setState({
                          referencePanelOpen: !this.state.referencePanelOpen,
                        })
                      }
                      onToggleVisualization={
                        this.state.showResultPane
                          ? () => this.setState({ showResultPane: false })
                          : null
                      }
                    />
                  </Box>
                </Flex>
                <Box
                  className="flex-full relative"
                  ref={node => (this.container = node)}
                >
                  <Motion
                    defaultStyle={{ x: 0, y: 0, opacity: 0 }}
                    style={{
                      x: vizPanelOpen ? spring(40) : spring(0),
                      y: spring(10),
                      opacity: vizPanelOpen ? spring(1) : spring(0),
                    }}
                  >
                    {({ x, y, opacity }) => (
                      <Absolute left={20} style={{ opacity, zIndex: 4 }}>
                        <VisualizationPanel />
                      </Absolute>
                    )}
                  </Motion>
                  <Motion
                    defaultStyle={{ x: 0, y: 0, opacity: 0 }}
                    style={{
                      x: referencePanelOpen ? spring(40) : spring(0),
                      y: spring(10),
                      opacity: referencePanelOpen ? spring(1) : spring(0),
                    }}
                  >
                    {({ x, y, opacity }) => (
                      <Absolute
                        right={40}
                        style={{
                          transform: `translate3d(${x}px, ${y}px, 0px)`,
                          opacity,
                          zIndex: 4,
                        }}
                      >
                        <ReferencePanel />
                      </Absolute>
                    )}
                  </Motion>
                  <Box style={{ height: "75vh" }}>
                    {rawSeries && <Visualization rawSeries={rawSeries} />}
                  </Box>
                  <Motion
                    defaultStyle={{ y: wrapperHeight - 60 }}
                    style={{
                      y: showResultPane
                        ? spring(0)
                        : spring(wrapperHeight - 60),
                    }}
                  >
                    {({ y }) => (
                      <Absolute
                        bottom={0}
                        left={0}
                        right={0}
                        top={0}
                        onClick={() =>
                          this.setState({
                            showResultPane: !this.state.showResultPane,
                          })
                        }
                        style={{
                          backgroundColor: "white",
                          transform: `translate3d(0px, ${y}px, 0px)`,
                        }}
                      >
                        <ResultPanel />
                      </Absolute>
                    )}
                  </Motion>
                </Box>
              </Box>
            );
          }}
        </QuestionAndResultLoader>
      </Flex>
    );
  }
}

export default ModeContainer;
